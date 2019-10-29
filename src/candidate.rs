use crate::*;

use logging::{debug, error, info, trace};
use std::collections::HashSet;
use std::cmp::max;
use rand::prelude::*;
use tokio::prelude::*;

struct Pending<'a> {
    id: &'a String,
    response: Box<VoteResponse>
}

pub struct State<'a> {
    votes: HashSet<&'a String>,
    ticks: usize,
    pending: Vec<Pending<'a>>
}

impl<'a> State<'a> {
    pub fn new () -> Self {
        State {
            votes: HashSet::new(),
            ticks: 0,
            pending: vec![]
        }
    }
}

pub fn become_candidate<'a, 'b, Record: Debug + Unique> (raft: &'a mut Raft<'b, Record>) {
    info!("Becoming Candidate");
    raft.role = Role::Candidate;
    start_election(raft);
}

pub fn start_election<'a, 'b, Record: Debug + Unique> (raft: &'a mut Raft<'b, Record>) {
    let term = raft.log.get_current_term() + 1;
    raft.log.set_current_term(term);
    trace!("starting new election, term {}", term);

    let last_log = raft.get_last_log_entry();

    let ref mut state = raft.volatile_state;
    let ref cluster = raft.cluster;

    let ref config = raft.config;
    let ref mut election = state.candidate;
    let jitter = random::<usize>() % config.election_restart_jitter;
    election.ticks = config.election_restart_ticks + jitter;

    let ref mut v: HashSet<&'b String> = election.votes;
    *v = HashSet::new();
    v.insert(&cluster.id);
    raft.log.set_voted_for(Some(cluster.id.to_string()));

    let link = &raft.link;

    election.pending = cluster.peers.iter().map(|id| {
        let response: Box<VoteResponse> = link.request_vote(id, RequestVote {
            candidate_id: cluster.id.to_string(),
            last_log: last_log.clone().unwrap_or(LogEntry { term: 0, index: 0 }),
            term: term
        });

        Pending { id: id, response: response }
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
            let id = p.id;
            match p.response.poll() {
                Ok(Async::Ready(message)) => {
                    highest_term = max(highest_term, message.term);
                    trace!("response: {:?}", message);
                    if message.vote_granted {
                        election.votes.insert(&id);
                    }
                }
                Err(message) => error!("RequestVote error: {}", message),
                _ => ()
            }
        }

        let votes_received = election.votes.len();
        let quorum = (raft.cluster.peers.len() / 2) + 1;
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
