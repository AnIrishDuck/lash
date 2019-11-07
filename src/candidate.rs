use crate::*;

use logging::{debug, error, info, trace};
use std::collections::HashSet;
use std::cmp::max;
use rand::prelude::*;
use tokio::prelude::*;

struct Pending {
    id: String,
    response: Box<VoteResponse>
}

pub struct State {
    votes: HashSet<String>,
    ticks: usize,
    pending: Vec<Pending>
}

impl State {
    pub fn new () -> Self {
        State {
            votes: HashSet::new(),
            ticks: 0,
            pending: vec![]
        }
    }
}

pub fn become_candidate<'a, 'b, Record: Debug + Unique> (raft: &mut Raft<'a, Record>) {
    info!("Becoming Candidate");
    raft.volatile_state.current_leader = None;
    raft.role = Role::Candidate;
    start_election(raft);
}

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

    let ref mut v: HashSet<String> = election.votes;
    *v = HashSet::new();
    v.insert(id.clone());
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
                        election.votes.insert(id.clone());
                    }
                }
                Err(message) => error!("RequestVote error: {}", message),
                _ => ()
            }
        }

        let votes_received = election.votes.len();
        let quorum = ((raft.cluster.new.peers.len() + 1) / 2) + 1;
        trace!(
            "current votes received: {}, quorum: {}, ticks left: {}",
            votes_received,
            quorum,
            election.ticks
        );

        (
            votes_received >= quorum,
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
