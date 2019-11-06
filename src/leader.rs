use crate::*;

use logging::{error, info, trace};
use std::cmp::max;
use tokio::prelude::*;

struct Follower {
    id: String,
    sent: usize,
    next_index: u64,
    // note: we stray slightly from the spec here. a "match index" might not
    // exist for the first leader when no followers have matching values.
    match_count: u64,
    pending: Option<Box<AppendResponse>>
}

pub struct State {
    followers: Vec<Follower>,
    learners: Vec<Follower>
}

impl State {
    pub fn new () -> Self {
        State {
            followers: vec![],
            learners: vec![]
        }
    }
}

fn init_followers(v: &Vec<String>, count: u64) -> Vec<Follower> {
    v.iter().map(|id| {
        Follower {
            id: id.clone(),
            sent: 0,
            next_index: count,
            match_count: 0,
            pending: None
        }
    }).collect()
}

pub fn become_leader<'a, Record: Unique> (raft: &mut Raft<'a, Record>) {
    let count = raft.volatile_state.commit_count;
    info!("Becoming Leader with consensus commit count {}", count);
    raft.role = Role::Leader;

    raft.volatile_state.leader = State {
        followers: init_followers(&raft.cluster.new.peers, count),
        learners: init_followers(&raft.cluster.new.learners, count)
    };
}

pub fn tick<'a, Record: Debug + Unique> (raft: &mut Raft<'a, Record>) {
    let term = raft.log.get_current_term();

    let mut highest_term = 0;
    {
        let ref mut leader = raft.volatile_state.leader;
        for ref mut follower in &mut leader.followers.iter_mut().chain(leader.learners.iter_mut()) {
            let send_more = match follower.pending {
                Some(ref mut response) => {
                    match response.poll() {
                        Ok(Async::NotReady) => false,
                        Ok(Async::Ready(append)) => {
                            highest_term = max(highest_term, append.term);
                            if append.success {
                                let sent = follower.sent as u64;
                                follower.next_index += sent;
                                follower.match_count = follower.next_index;
                                trace!(
                                    "appended {} to {} (count {}, next index {})",
                                    sent,
                                    follower.id,
                                    follower.match_count,
                                    follower.next_index
                                );
                            } else {
                                if follower.next_index > 0 {
                                    follower.next_index -= 1;
                                }

                                trace!(
                                    "{} rejected append, rewinding (now at {})",
                                    follower.id,
                                    follower.next_index
                                );
                            }
                            true
                        },
                        Err(string) => {
                            error!("AppendEntries error: {}", string);
                            true
                        }
                    }
                },
                None => true
            };

            if send_more {
                let my_count = raft.log.get_count();
                let missing_entries = follower.next_index < my_count;

                let prior_index = if follower.next_index > 0 {
                    Some(follower.next_index - 1)
                } else {
                    None
                };

                let prior_entry = match prior_index {
                    Some(index) => {
                        let entry = raft.log.get_entry(index);
                        match entry {
                            Some((entry_term, _)) =>
                                Some(LogEntry {
                                    index: index,
                                    term: entry_term
                                }),
                            None => {
                                Some(LogEntry {
                                    index: index,
                                    term: term
                                })
                            }
                        }
                    },
                    None => None
                };
                let records = raft.log.get_batch(follower.next_index);

                follower.sent = records.len();
                trace!(
                    "sending {} to {} (prior {:?}, follower has {})",
                    follower.sent,
                    follower.id,
                    prior_entry,
                    follower.next_index
                );

                let response = raft.link.append_entries(&follower.id, AppendEntries {
                    term: term,
                    previous_entry: prior_entry,
                    entries: records,
                    leader_commit: raft.volatile_state.commit_count
                });

                follower.pending = Some(response);
            }
        }

    }

    if highest_term > term {
        raft.check_term(term, true);
    } else {
        let ref leader = raft.volatile_state.leader;
        let mut matches: Vec<u64> = leader.followers.iter().map(|follower| {
            count_to_index(follower.match_count).map(|index| {
                let entry = raft.log.get_entry(index);
                if entry.map(|(t, _)| t).unwrap_or(0) == term {
                    follower.match_count
                } else { 0 }
            }).unwrap_or(0)
        }).collect();
        matches.sort_unstable();

        let ref commit_count = raft.volatile_state.commit_count;
        let commit = *commit_count;
        let middle = matches.len() / 2;
        let next_commit = max(matches[middle], commit);

        trace!("log counts: {:?}; consensus count: {}", matches, commit);
        raft.volatile_state.commit_count = next_commit;
    }
}
