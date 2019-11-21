use crate::*;

use logging::{debug, error, info, trace};
use std::collections::HashSet;
use std::cmp::max;
#[cfg(not(feature = "old_futures"))]
use futures::task::Context;
#[cfg(not(feature = "old_futures"))]
use futures::stream::futures_unordered::FuturesUnordered;
use rand::prelude::*;
#[cfg(not(feature = "old_futures"))]
use futures::stream::StreamExt;

#[cfg(feature = "old_futures")]
struct Pending {
    id: String,
    response: Box<VoteResponse>
}

pub struct State {
    new_votes: HashSet<String>,
    old_votes: HashSet<String>,
    ticks: usize,
    #[cfg(feature = "old_futures")]
    pending: Vec<Pending>
}

impl State {
    pub fn new () -> Self {
        State {
            new_votes: HashSet::new(),
            old_votes: HashSet::new(),
            ticks: 0,
            #[cfg(feature = "old_futures")]
            pending: vec![]
        }
    }
}

#[cfg(feature = "old_futures")]
pub fn become_candidate<'a, 'b, Record: Debug + Unique> (raft: &mut Raft<'a, Record>) {
    info!("Becoming Candidate");
    raft.volatile_state.current_leader = None;
    raft.role = Role::Candidate;
    start_election(raft);
}

fn init_votes(id: &String) -> HashSet<String> {
    let mut votes = HashSet::new();
    votes.insert(id.clone());
    votes
}

#[cfg(feature = "old_futures")]
pub fn start_election<'a, 'b, Record: Debug + Unique> (raft: &mut Raft<'a, Record>) {
    let id = raft.id.clone();
    let term = raft.log.get_current_term() + 1;
    raft.log.set_current_term(term);
    trace!("starting new election, term {}", term);

    let last_log = raft.get_last_log_entry();

    let ref mut state = raft.volatile_state;
    let ref cluster = raft.cluster.new;

    let ref config = raft.config;
    let ref mut election = state.candidate;
    let jitter = random::<usize>() % config.election_restart_jitter;
    election.ticks = config.election_restart_ticks + jitter;

    election.old_votes = init_votes(&id);
    election.new_votes = init_votes(&id);
    raft.log.set_voted_for(Some(id.clone()));

    let link = &raft.link;

    election.pending = cluster.peers.iter().map(|peer_id| {
        let response: Box<VoteResponse> = link.request_vote(peer_id, RequestVote {
            candidate_id: id.clone(),
            last_log: last_log.clone().unwrap_or(LogEntry { term: 0, index: 0 }),
            term: term
        });

        Pending { id: peer_id.clone(), response: response }
    }).collect();
}

#[cfg(not(feature = "old_futures"))]
pub async fn election<'a, 'b, Record: Debug + Unique> (raft: &mut Raft<'a, Record>) -> Role {
    let id = raft.id.clone();
    let term = raft.log.get_current_term() + 1;
    raft.log.set_current_term(term);
    trace!("starting new election, term {}", term);

    let last_log = raft.get_last_log_entry();

    let ref mut state = raft.volatile_state;
    let ref cluster = raft.cluster.new;

    let ref config = raft.config;
    let ref mut election = state.candidate;
    let jitter = random::<usize>() % config.election_restart_jitter;
    election.ticks = config.election_restart_ticks + jitter;

    election.old_votes = init_votes(&id);
    election.new_votes = init_votes(&id);
    raft.log.set_voted_for(Some(id.clone()));

    let link = &raft.link;

    let responses: FuturesUnordered<_> = cluster.peers.iter().map(|peer_id| {
        use future::FutureExt;
        let response: Pin<Box<VoteResponse>> = Box::pin(link.request_vote(peer_id, RequestVote {
            candidate_id: id.clone(),
            last_log: last_log.clone().unwrap_or(LogEntry { term: 0, index: 0 }),
            term: term
        }));

        response.map(move |v| ((*peer_id).clone(), v))
    }).collect();

    let mut has_quorum = false;
    let cluster = raft.cluster.clone();

    let votes = responses.take_while(|(peer_id, message)| {
        let vote_granted = message.as_ref().map(|r| r.vote_granted).unwrap_or(false);
        if vote_granted {
            if cluster.old.as_ref().map(|l| l.has_peer(&peer_id)).unwrap_or(false) {
                election.old_votes.insert(peer_id.clone());
            }
            if cluster.new.has_peer(&peer_id) {
                election.new_votes.insert(peer_id.clone());
            }

            let old_quorum = cluster.old.as_ref().map(
                |l| check_quorum("old", &election.old_votes, l)
            ).unwrap_or(false);
            let new_quorum = check_quorum("new", &election.new_votes, &cluster.new);
            has_quorum = old_quorum || new_quorum;
            future::ready(!has_quorum)
        } else {
            future::ready(true)
        }
    });

    votes.collect::<Vec<_>>().await;

    if has_quorum { Role::Leader } else { Role::Candidate }
}

pub fn check_quorum(list: &str, votes: &HashSet<String>, nodes: &NodeList) -> bool {
    let votes_received = votes.len();
    let quorum = ((nodes.peers.len() + 1) / 2) + 1;
    trace!(
        "current {} votes received: {}, quorum: {}",
        list,
        votes_received,
        quorum,
    );
    votes_received >= quorum
}

#[cfg(feature = "old_futures")]
pub fn tick<'a, Record: Debug + Unique> (raft: &mut Raft<'a, Record>) {
    let term = raft.log.get_current_term();
    let mut highest_term = 0;

    let (majority, timeout) = {
        let ref mut state = raft.volatile_state;
        let ref mut election = state.candidate;

        election.ticks -= 1;

        for p in &mut election.pending {
            let id = p.id.clone();
            match p.response.poll() {
                Ok(Async::Ready(message)) => {
                    highest_term = max(highest_term, message.term);
                    trace!("response: {:?}", message);
                    if message.vote_granted {
                        if raft.cluster.old.as_ref().map(|l| l.has_peer(&id)).unwrap_or(false) {
                            election.old_votes.insert(id.clone());
                        }
                        if raft.cluster.new.has_peer(&id) {
                            election.new_votes.insert(id.clone());
                        }
                    }
                }
                Err(message) => error!("RequestVote error: {}", message),
                _ => ()
            }
        }

        let old_quorum = raft.cluster.old.as_ref().map(
            |l| check_quorum("old", &election.old_votes, l)
        ).unwrap_or(false);
        let new_quorum = check_quorum("new", &election.new_votes, &raft.cluster.new);
        trace!("ticks left: {}", election.ticks);

        (
            old_quorum || new_quorum,
            election.ticks == 0
        )
    };

    if highest_term > term {
        raft.check_term(highest_term, false);
    } else if majority {
        leader::become_leader(raft);
    } else {
        if timeout {
            debug!("election timed out, restarting");
            start_election(raft);
        }
    }
}

#[cfg(all(test, not(feature = "old_futures")))]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use futures::executor::block_on;
    use crate::log::MemoryLog;
    use std::cell::RefCell;

    extern crate env_logger;

    struct FakeLink {
        appends: RefCell<HashMap<String, Box<AppendResponse>>>,
        votes: RefCell<HashMap<String, Box<VoteResponse>>>,
    }

    impl<Record> Link<Record> for FakeLink {
        fn append_entries(&self, id: &String, _request: AppendEntries<Record>) -> Box<AppendResponse> {
            self.appends.borrow_mut().remove(id).unwrap()
        }

        fn request_vote (&self, id: &String, _request: RequestVote) -> Box<VoteResponse> {
            self.votes.borrow_mut().remove(id).unwrap()
        }
    }

    fn immediate_vote(v: Vote) -> Box<VoteResponse> {
        Box::new(future::ok(v))
    }

    fn missing_vote() -> Box<VoteResponse> {
        Box::new(future::pending())
    }

    #[test]
    fn elects_leader() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let log: MemoryLog<u64> = MemoryLog::new();
        let votes: HashMap<String, Box<VoteResponse>> = vec![("a", true), ("b", true), ("c", false)].iter()
            .map(|(id, g)| (id.to_string(), immediate_vote(Vote { term: 0, vote_granted: *g }))).collect();
        let link: FakeLink = FakeLink {
            appends: RefCell::new(HashMap::new()),
            votes: RefCell::new(votes)
        };
        {
            let mut raft: Raft<u64> = Raft::new(id, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));
            raft.force_peers(NodeList {
                peers: vec!["a", "b", "c"].iter().map(|v| v.to_string()).collect(),
                learners: vec![]
            });

            assert_eq!(block_on(election(&mut raft)), Role::Leader);
        }
    }

    #[test]
    fn remains_candidate_on_failed_election() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let log: MemoryLog<u64> = MemoryLog::new();
        let votes: HashMap<String, Box<VoteResponse>> = vec![("a", true), ("b", false), ("c", false)].iter()
            .map(|(id, g)| (id.to_string(), immediate_vote(Vote { term: 0, vote_granted: *g }))).collect();
        let link: FakeLink = FakeLink {
            appends: RefCell::new(HashMap::new()),
            votes: RefCell::new(votes)
        };
        {
            let mut raft: Raft<u64> = Raft::new(id, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));
            raft.force_peers(NodeList {
                peers: vec!["a", "b", "c"].iter().map(|v| v.to_string()).collect(),
                learners: vec![]
            });

            assert_eq!(block_on(election(&mut raft)), Role::Candidate);
        }
    }

    #[test]
    fn remains_pending_without_votes() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let log: MemoryLog<u64> = MemoryLog::new();
        let votes: HashMap<String, Box<VoteResponse>> = vec![("a", true), ("b", false), ("c", false)].iter()
            .map(|(id, present)| {
                (
                    id.to_string(),
                    if *present {
                        immediate_vote(Vote { term: 0, vote_granted: true })
                    } else {
                        missing_vote()
                    }
                )
            }).collect();

        let link: FakeLink = FakeLink {
            appends: RefCell::new(HashMap::new()),
            votes: RefCell::new(votes)
        };

        {
            let mut raft: Raft<u64> = Raft::new(id, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));
            raft.force_peers(NodeList {
                peers: vec!["a", "b", "c"].iter().map(|v| v.to_string()).collect(),
                learners: vec![]
            });

            use crate::future::FutureExt;
            assert_eq!(election(&mut raft).now_or_never(), None);
        }
    }
}

#[cfg(all(test, feature = "old_futures"))]
mod tests {
    use super::*;
    use crate::log::MemoryLog;

    extern crate env_logger;

    fn nodes(l: Vec<&str>) -> Vec<String> {
        l.iter().map(|v| v.to_string()).collect()
    }

    fn setup_votes(raft: &mut Raft<u64>, old: Vec<&str>, new: Vec<&str>) {
        raft.cluster = ClusterConfig {
            old: Some(NodeList {
                peers: nodes(vec!["a", "b", "c", "d"]),
                learners: vec![]
            }),
            new: NodeList {
                peers: nodes(vec!["a", "e", "f", "g"]),
                learners: vec![]
            },
            id: String::from("config")
        };

        raft.volatile_state.candidate = State {
            old_votes: old.iter().map(|v| v.to_string()).collect(),
            new_votes: new.iter().map(|v| v.to_string()).collect(),
            pending: vec![],
            ticks: 20,
        };
    }

    #[test]
    fn uses_separate_dual_quorum() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(id, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));
            become_candidate(&mut raft);
            setup_votes(&mut raft, vec!["me", "b"], vec!["me", "e"]);

            tick(&mut raft);

            assert_eq!(raft.role, Role::Candidate)
        }
    }

    #[test]
    fn promotes_via_new_quorum() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(id, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));
            become_candidate(&mut raft);
            setup_votes(&mut raft, vec!["me", "b"], vec!["me", "e", "f"]);

            tick(&mut raft);

            assert_eq!(raft.role, Role::Leader)
        }
    }

    #[test]
    fn promotes_via_old_quorum() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(id, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));
            become_candidate(&mut raft);
            setup_votes(&mut raft, vec!["me", "b", "c"], vec!["me", "e"]);

            tick(&mut raft);

            assert_eq!(raft.role, Role::Leader)
        }
    }
}
