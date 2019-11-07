extern crate log as logging;
extern crate env_logger;
extern crate rand;
extern crate tokio;
extern crate futures;

use logging::{debug, error, trace};

use tokio::prelude::Async;
use futures::{future, Future};
use std::sync::mpsc::{channel, Receiver, Sender, TryRecvError};
use std::collections::BTreeMap;
use std::cmp::min;
use std::fmt::Debug;

mod follower;
mod candidate;
mod leader;
pub mod log;

/* (§8) all records must bear a unique identifier, to handle the following case:
 *  - leader receives request from client
 *  - leader commits request and persists to cluster with AppendEntries
 *  - leader fails before returning success to client
 *  - client retries with new leader that already has proposal
 */
pub trait Unique {
    fn id (&self) -> String;
}

#[derive(Debug, Clone)]
pub enum LogData<Record> {
    Entry(Record),
    ClusterChange(ClusterConfig)
}

/* prelude: definitions from page 4 of the raft paper */
pub trait Log<Record: Unique> {
    fn get_current_term (&self) -> u64;
    fn set_current_term (&mut self, term: u64);

    fn get_voted_for (&self) -> Option<String>;
    fn set_voted_for (&mut self, candidate: Option<String>);

    fn get_count (&self) -> u64;
    fn get_entry (&self, index: u64) -> Option<(u64, Box<LogData<Record>>)>;
    fn insert (&mut self, index: u64, records: Vec<(u64, Box<LogData<Record>>)>);

    fn lookup_id (&self, id: &String) -> Option<u64>;

    fn get_batch (&self, index: u64) -> Vec<(u64, Box<LogData<Record>>)>;
}

pub trait StateMachine<Record> {
    fn apply (record: Box<Record>) -> bool;
}

pub fn count_to_index(count: u64) -> Option<u64> {
    if count > 0 { Some(count - 1) } else { None }
}

pub struct VolatileState {
    // note: we stray slightly from the spec here. a "commit index" might not
    // exist in the startup state where no values have been proposed.
    pub commit_count: u64,
    pub pending: BTreeMap<u64, Sender<bool>>,
    pub current_leader: Option<String>,
    // we will track last_applied in the state machine
    candidate: candidate::State,
    leader: leader::State,
    follower: follower::State
}

#[derive(Debug, Clone)]
pub struct NodeList {
    pub peers: Vec<String>,
    pub learners: Vec<String>
}

#[derive(Debug, Clone)]
pub struct ClusterConfig {
    pub old: Option<NodeList>,
    pub new: NodeList
}

impl ClusterConfig {
    pub fn empty() -> Self {
        ClusterConfig {
            old: None,
            new: NodeList {
                peers: vec![],
                learners: vec![]
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.old.is_none() && self.new.peers.is_empty() && self.new.learners.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64
}

#[derive(Debug, Clone)]
pub struct AppendEntries<Record> {
    term: u64,
    // we never ended up needing leader_id
    // we deviate from the spec here for clarity: there might be no prior entry
    // so we might not have anything to synchronize with
    previous_entry: Option<LogEntry>,
    entries: Vec<(u64, Box<LogData<Record>>)>,
    leader_commit: u64
}

#[derive(Debug, Clone)]
pub struct Append {
    term: u64,
    success: bool
}

pub type AppendResponse = Future<Item=Append, Error=String>;

#[derive(Debug, Clone)]
pub struct RequestVote {
    pub term: u64,
    pub candidate_id: String,
    pub last_log: LogEntry
}

#[derive(Debug, Clone)]
pub struct Vote {
    term: u64,
    pub vote_granted: bool
}

pub type VoteResponse = Future<Item=Vote, Error=String>;

pub trait Link<Record> {
    fn append_entries(&self,id: &String, request: AppendEntries<Record>) -> Box<AppendResponse>;

    fn request_vote (&self, id: &String, request: RequestVote) -> Box<VoteResponse>;
}

#[derive(Debug, PartialEq, Clone)]
pub enum Role { Follower, Candidate, Leader, Learner }

#[derive(Clone)]
pub struct Config {
    election_restart_ticks: usize,
    election_restart_jitter: usize
}

pub static DEFAULT_CONFIG: Config = Config {
    election_restart_ticks: 10,
    election_restart_jitter: 5
};

pub struct Raft<'a, Record: Unique + 'a> {
    pub id: String,
    config: Config,
    pub cluster: ClusterConfig,
    pub volatile_state: VolatileState,
    log: Box<Log<Record> + 'a>,
    link: Box<Link<Record> + 'a>,
    pub role: Role
}

impl<'a, Record: Unique + Debug + 'a> Raft<'a, Record> {
    pub fn new (id: String, config: Config, log: Box<Log<Record> + 'a>, link: Box<Link<Record> + 'a>) -> Self {
        let volatile = VolatileState {
            candidate: candidate::State::new(),
            commit_count: 0,
            current_leader: None,
            follower: follower::State::new(),
            leader: leader::State::new(),
            pending: BTreeMap::new()
        };

        Raft {
            id: id,
            config: config,
            cluster: ClusterConfig::empty(),
            link: link,
            log: log,
            role: Role::Follower,
            volatile_state: volatile
        }
    }

    pub fn force_peers(&mut self, peers: NodeList) {
        self.cluster = ClusterConfig { old: None, new: peers }
    }

    pub fn check_term(&mut self, message_term: u64, append: bool) -> u64 {
        let term = self.log.get_current_term();
        // # Rules for Servers / All Servers
        // If RPC request or response contains term T > currentTerm:
        // set currentTerm = T, convert to follower (§5.1)
        let new_leader = message_term > term && self.role != Role::Learner;
        // # Rules for Servers / Candidates:
        // If AppendEntries RPC received from new leader: convert to follower
        let candidate = self.role == Role::Candidate;

        let election_lost = candidate && message_term == term && append;
        if new_leader || election_lost {
            if new_leader {
                trace!("reset with term {}", message_term);
            } else {
                trace!("lost election for term {}", message_term);
            }

            for (_, send) in self.volatile_state.pending.iter() {
                send.send(false);
            }
            self.volatile_state.pending = BTreeMap::new();

            self.log.set_current_term(message_term);
            follower::become_follower(self);
            message_term
        } else {
            term
        }
    }

    pub fn append_entries (&mut self, source: String, request: AppendEntries<Record>) -> Append {
        let current_term = self.check_term(request.term, true);
        let count = request.entries.len() as u64;
        debug!(
            "RX AppendEntries: {} at index {:?}",
            count,
            request.previous_entry,
        );

        let success = match self.role {
            Role::Follower | Role::Learner => {
                self.volatile_state.current_leader = Some(source);
                follower::append_entries(self, request)
            },
            _ => false
        };
        let response = Append { term: current_term, success: success };

        debug!("TX: {:?}", response);
        response
    }

    pub fn request_vote (&mut self, request: RequestVote) -> Vote {
        let current_term = self.check_term(request.term, false);

        debug!("RX: {:?}", request);
        let vote_granted = if request.term < current_term { false } else {
            let prior_vote = {
                let voted_for = self.log.get_voted_for();

                trace!("prior vote: {:?}", voted_for);
                match voted_for {
                    Some(ref vote) => *vote == request.candidate_id,
                    None => true
                }
            };

            let log_current = self.get_last_log_entry().map(|last| {
                trace!("last log entry: {:?}", last);
                if request.last_log.term == last.term {
                    request.last_log.index >= last.index
                } else { request.last_log.term > last.term }
            }).unwrap_or(true);

            prior_vote && log_current
        };

        if vote_granted {
            self.log.set_voted_for(Some(request.candidate_id));
        }

        let response = Vote {
            term: current_term,
            vote_granted: vote_granted
        };
        debug!("TX: {:?}", response);
        response
    }

    pub fn get_propose_index (&mut self, r: Box<Record>) -> Option<u64> {
        match self.role {
            Role::Leader => {
                Some(
                    self.log.lookup_id(&r.id()).unwrap_or_else(|| {
                        let term = self.log.get_current_term();
                        let count = self.log.get_count();
                        trace!("Leader recording proposal {:?} => {}", r.id(), count);
                        self.log.insert(count, vec![(term, Box::new(LogData::Entry(*r)))]);
                        count
                    })
                )
            },
            _ => None
        }
    }

    pub fn propose (&mut self, r: Box<Record>) -> FutureProgress {
        let proposal_valid = self.get_propose_index(r);
        let (send, recv) = channel();

        match proposal_valid {
            Some(ix) => {
                if ix < self.volatile_state.commit_count { send.send(true).unwrap() }
                else {
                    self.volatile_state.pending.insert(ix, send);
                }
            },
            None => { send.send(false).unwrap() }
        }

        FutureProgress {
            valid: recv,
            ix: proposal_valid
        }
    }

    fn get_last_log_entry<'b> (&'b mut self) -> Option<LogEntry> {
        let final_index = count_to_index(self.log.get_count());
        final_index.and_then(|index| {
            self.log.get_entry(index).map(|(term, _)| {
                LogEntry { index: index, term: term }
            })
        })
    }

    pub fn tick (&mut self) {
        let prior_commit = { self.volatile_state.commit_count };
        match self.role {
            Role::Follower => follower::tick(self),
            Role::Candidate => candidate::tick(self),
            Role::Leader => leader::tick(self),
            Role::Learner => ()
        }

        let current_commit = { self.volatile_state.commit_count };
        for ix in prior_commit..current_commit {
            let rm = match self.volatile_state.pending.get(&ix) {
                Some(channel) => { channel.send(true); true },
                None => false
            };

            if rm { self.volatile_state.pending.remove(&ix); }
        }
    }
}

pub struct FutureProgress {
    valid: Receiver<bool>,
    ix: Option<u64>
}

impl Future for FutureProgress
{
    type Item = u64;
    type Error = String;

    fn poll (&mut self) -> Result<Async<u64>, String> {
        match self.valid.try_recv() {
            Ok(valid) => {
                if valid {
                    Ok(Async::Ready(self.ix.unwrap()))
                } else {
                    Err("cluster change".to_string())
                }
            },
            Err(TryRecvError::Empty) => Ok(Async::NotReady),
            Err(TryRecvError::Disconnected) => Err("raft destroyed".to_string())
        }
    }
}

pub struct NullLink {}

impl NullLink {
    pub fn new () -> Self {
        NullLink { }
    }
}

impl<Record> Link<Record> for NullLink {
    fn append_entries(&self, _id: &String, _request: AppendEntries<Record>) -> Box<AppendResponse> {
        Box::new(future::ok(Append { term: 0, success: false }))
    }

    fn request_vote (&self, _id: &String, _request: RequestVote) -> Box<VoteResponse> {
        Box::new(future::ok(Vote { term: 0, vote_granted: false }))
    }
}
