use crate::*;
use logging::{info, trace};

pub struct State {
    heartbeat_ticks: u64
}

impl State {
    pub fn new () -> Self {
        State { heartbeat_ticks: 0 }
    }
}

pub fn become_follower<Record: Unique> (raft: &mut Raft<Record>) {
    raft.volatile_state.follower = State::new();
    raft.log.set_voted_for(None);
    raft.role = Role::Follower;
}

pub fn tick<'a, Record: Debug + Unique> (raft: &mut Raft<'a, Record>) {
    let ticks = {
        let ref mut ticks = raft.volatile_state.follower.heartbeat_ticks;
        *ticks += 1;
        *ticks
    };

    let timeout = raft.config.election_restart_ticks as u64;
    if ticks > timeout {
        info!("Leader timed out, becoming candidate");
        candidate::become_candidate(raft);
    }
}

pub fn append_entries<Record: Unique> (raft: &mut Raft<Record>, request: AppendEntries<Record>) -> bool {
    let current_term = raft.log.get_current_term();
    let mut my_count = raft.log.get_count();

    raft.volatile_state.follower.heartbeat_ticks = 0;

    let request_from_prior_term = request.term < current_term;
    let prior_index = request.previous_entry;

    let inconsistent = prior_index.as_ref().map(|entry| {
        raft.log.get_entry(entry.index).map(|(check_term, _)| {
            trace!("Prior entry term: {}", check_term);
            check_term != entry.term
        }).unwrap_or(true)
    }).unwrap_or(false);

    let success = if request_from_prior_term || inconsistent {
        false
    } else {
        let next_index = prior_index.map(|e| e.index + 1).unwrap_or(0);

        raft.log.insert(next_index, request.entries);
        my_count = raft.log.get_count();
        trace!("Log updated; new size: {}", my_count);

        let leader_count = request.leader_commit;
        {
            let commit_count = &raft.volatile_state.commit_count;
            if leader_count > *commit_count {
                let min_count = min(leader_count, my_count);
                raft.volatile_state.commit_count = min_count;
                trace!(
                    "Leader commit count {}, my commit count: {}",
                    leader_count,
                    min_count
                );
            }
        }

        true
    };

    success
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::MemoryLog;

    extern crate env_logger;


    fn single_node_cluster<'a> (id: &'a String) -> Cluster {
        Cluster {
            id: id.clone(),
            peers: vec![id.clone()]
        }
    }

    fn boxed(raw: Vec<(u64, u64)>) -> Vec<(u64, Box<LogData<u64>>)> {
        raw.iter().map(|(t, v)| (*t, Box::new(LogData::Entry(*v)))).collect()
    }

    impl Unique for u64 {
        fn id(&self) -> String {
            self.to_string()
        }
    }

    #[test]
    fn append_entries_from_empty() {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let cluster = single_node_cluster(&id);
        let log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(cluster, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));

            let response = raft.append_entries("leader".to_string(), AppendEntries {
                term: 0,
                previous_entry: None,
                entries: boxed(vec![(0, 1), (0, 2), (0, 3)]),
                leader_commit: 10
            });

            assert_eq!(response.term, 0);
            assert_eq!(response.success, true);
            assert_eq!(raft.volatile_state.commit_count, 3);
        }
        assert_eq!(log.record_vec(), vec![(0, 1), (0, 2), (0, 3)]);
    }

    fn fill_term (raft: &mut Raft<u64>, term: u64, prior: Option<LogEntry>, count: u64) {
        let mut fill = Vec::new();
        fill.resize(count as usize, (term, 0));
        let response = raft.append_entries("leader".to_string(), AppendEntries {
            term: term,
            previous_entry: prior,
            entries: boxed(fill),
            leader_commit: 20
        });
        assert_eq!(response.success, true);
    }

    #[test]
    fn append_inconsistent_entries () {
        let _ = env_logger::try_init();
        let id = "me".to_owned();
        let cluster = single_node_cluster(&id);
        let log: MemoryLog<u64> = MemoryLog::new();
        let link: NullLink = NullLink::new();
        {
            let mut raft: Raft<u64> = Raft::new(cluster, DEFAULT_CONFIG.clone(), Box::new(log.clone()), Box::new(link));

            let response = raft.append_entries("leader".to_string(), AppendEntries {
                term: 0,
                previous_entry: Some(LogEntry { term: 0, index: 5 }),
                entries: boxed(vec![(0, 1),(0, 2),(0, 3)]),
                leader_commit: 0
            });
            assert_eq!(response.success, false);

            // see example f, figure 7 (page 7)
            fill_term(&mut raft, 1, None, 3);
            fill_term(&mut raft, 2, Some(LogEntry { term: 1, index: 2 }), 3);
            fill_term(&mut raft, 3, Some(LogEntry { term: 2, index: 5 }), 3);

            // per the protocol, a new leader initially would attempt to append
            // results from term 6
            let response = raft.append_entries("leader".to_string(), AppendEntries {
                term: 8,
                previous_entry: Some(LogEntry { term: 5, index: 6 }),
                entries: boxed(vec![(6, 1),(6, 2),(6, 3)]),
                leader_commit: 0
            });
            assert_eq!(response.success, false);

            // the leader would then iterate backwards until finding the index
            // where the logs are consistent
            let response = raft.append_entries("leader".to_string(), AppendEntries {
                term: 8,
                previous_entry: Some(LogEntry { term: 1, index: 2 }),
                entries: boxed(vec![(4, 1), (4, 2)]),
                leader_commit: 0
            });
            assert_eq!(response.success, true);
        }

        assert_eq!(log.record_vec(), vec![
            (1, 0),
            (1, 0),
            (1, 0),
            (4, 1),
            (4, 2)
        ]);
    }
}
