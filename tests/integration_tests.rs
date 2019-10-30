extern crate log as logging;

#[cfg(test)]
mod tests {
    use lash::*;
    use lash::NullLink;
    use lash::log::MemoryLog;
    use rand::prelude::*;
    use std::collections::{HashMap, HashSet};
    use std::cell::RefCell;
    use std::rc::Rc;
    use tokio::prelude::*;

    use logging::{debug, info, trace};

    extern crate env_logger;

    #[derive(Clone, Debug, PartialEq)]
    struct Record(u64);

    impl Unique for Record {
        fn id(&self) -> String {
            match self {
                Record(v) => v.to_string()
            }
        }
    }

    #[derive(Clone)]
    struct Call<Request, Response> {
        request: Request,
        response: Rc<RefCell<Option<Result<Response, String>>>>
    }

    impl<Request, Response> Call<Request, Response> {
        fn new(r: Request) -> Self {
            Call {
                request: r,
                response: Rc::new(RefCell::new(None))
            }
        }

        fn resolve(&self, r: Result<Response, String>) {
            *self.response.borrow_mut() = Some(r);
        }
    }

    impl<X, T: Clone> Future for Call<X, T>
    {
        type Item = T;
        type Error = String;

        fn poll (&mut self) -> Result<Async<T>, String> {
            match self.response.borrow().as_ref() {
                Some(Ok(r)) => {
                    let o: T = r.clone();
                    Ok(Async::Ready(o))
                },
                Some(Err(e)) => Err(e.clone()),
                None => Ok(Async::NotReady)
            }
        }
    }

    type Incoming<Request, Response> = Rc<RefCell<HashMap<
        String, Call<Request, Response>
    >>>;

    #[derive(Clone)]
    struct SwitchLink {
        id: String,
        append: Incoming<AppendEntries<Record>, Append>,
        vote: Incoming<RequestVote, Vote>,
    }

    impl SwitchLink {
        fn new (id: &String) -> Self {
            SwitchLink {
                id: id.clone(),
                append: Rc::new(RefCell::new(HashMap::new())),
                vote: Rc::new(RefCell::new(HashMap::new()))
            }
        }
    }

    impl Link<Record> for SwitchLink {
        fn append_entries(&self, id: &String, r: AppendEntries<Record>) -> Box<AppendResponse> {
            trace!("{} => {:?} => {}", self.id, r, id);
            let ref mut calls = self.append.borrow_mut();
            assert!(!calls.contains_key(id));
            let call = Call::new(r);
            calls.insert(id.clone(), call.clone());
            Box::new(call)
        }

        fn request_vote (&self, id: &String, r: RequestVote) -> Box<VoteResponse> {
            trace!("{} => {:?} => {}", self.id, r, id);
            let ref mut calls = self.vote.borrow_mut();
            assert!(!calls.contains_key(id));
            let call = Call::new(r);
            calls.insert(id.clone(), call.clone());
            Box::new(call)
        }
    }

    struct Node<'a> {
        raft: Raft<'a, Record>,
        link: SwitchLink,
        log: MemoryLog<Record>
    }

    struct Switchboard<'a> {
        nodes: HashMap<String, RefCell<Node<'a>>>,
        tick: RefCell<u64>
    }

    fn others<'a> (id: &'a String, ids: &Vec<&'a String>) -> Vec<&'a String> {
        ids.iter().filter(|peer_id| **peer_id != id).map(|i| i.clone()).collect()
    }

    impl<'a> Switchboard<'a> {
        fn new (ids: Vec<&'a String>) -> Self {
            let nodes: HashMap<String, RefCell<Node<'a>>> = ids.iter().map(|id| {
                let cluster: Cluster<'a> = Cluster {
                    id: id,
                    peers: others(id, &ids)
                };
                let log = MemoryLog::new();
                let link = SwitchLink::new(&id);
                let raft = Raft::new(
                    cluster,
                    &DEFAULT_CONFIG,
                    Box::new(log.clone()),
                    Box::new(link.clone())
                );

                let n: Node<'a> = Node {
                    raft: raft,
                    link: link,
                    log: log
                };

                (
                    id.to_string(),
                    RefCell::new(n)
                )
            }).collect();

            Switchboard {
                nodes: nodes,
                tick: RefCell::new(0)
            }
        }

        fn tick (&self) {
            let mut tick = self.tick.borrow_mut();
            debug!("tick {}", tick);
            *tick = *tick + 1;
            for node in self.nodes.values() {
                let ref mut n = node.borrow_mut();
                let ref mut raft: Raft<'a, _> = n.raft;
                raft.tick();
            }

            // Election Safety: at most one leader can be elected in a given
            // term. §5.2
            let leaders = self.leaders();
            let leader_count = leaders.len();
            let term_set: HashSet<_> = leaders.into_iter().map(|id|
                self.nodes.get(id).unwrap().borrow().log.get_current_term()
            ).collect();
            assert!(term_set.len() == leader_count);
        }

        fn process_all_messages(&self) {
            let always: Box<Fn(String, String) -> bool> = Box::new(|_, _| true);
            self.process_messages(&always)
        }

        fn process_messages (&self, arbiter: &Box<Fn(String, String) -> bool>) {

            for node in self.nodes.values() {
                {
                    let mut inner = node.borrow_mut();
                    let append = inner.link.append.borrow();
                    for (id, call) in append.iter() {
                        let rx = id;
                        let tx = &inner.link.id;
                        let request = call.request.clone();

                        let mut other = self.nodes.get(id).unwrap().borrow_mut();

                        let result = if arbiter(tx.clone(), rx.clone()) {
                            trace!("resolving append {} => {}", tx, rx);
                            Ok(other.raft.append_entries(request))
                        } else {
                            trace!("rejecting append {} => {}", tx, rx);
                            Err("rejection hurts".to_owned())
                        };
                        call.resolve(result);
                    }
                }

                {
                    let mut inner = node.borrow_mut();
                    let votes = inner.link.vote.borrow();
                    for (id, call) in votes.iter() {
                        let rx = id;
                        let tx = &inner.link.id;
                        let request = call.request.clone();

                        let mut other = self.nodes.get(id).unwrap().borrow_mut();


                        let result = if arbiter(tx.clone(), rx.clone()) {
                            trace!("resolving vote {} => {}", tx, rx);
                            Ok(other.raft.request_vote(request))
                        } else {
                            trace!("rejecting vote {} => {}", tx, rx);
                            Err("rejection hurts".to_owned())
                        };
                        call.resolve(result);
                    }
                }

                {
                    let mut inner = node.borrow();
                    *inner.link.vote.borrow_mut() = HashMap::new();
                    *inner.link.append.borrow_mut() = HashMap::new();
                }
            }
        }

        fn leaders (&self) -> Vec<&'a String> {
            self.nodes.values().flat_map(|n| {
                let ref raft = n.borrow().raft;
                if raft.role == Role::Leader {
                    let x: &'a String = raft.cluster.id;
                    Some(x)
                } else {
                    None
                }
            }).collect()
        }

        fn leader (&self) -> Option<&'a String> {
            self.leaders().get(0).map(|id| *id)
        }
    }


    fn single_node_cluster<'a> (id: &'a String) -> Cluster<'a> {
        Cluster {
            id: &id,
            peers: vec![&id]
        }
    }

    fn ignore(ids: HashSet<String>) -> Box<Fn(String, String) -> bool> {
        Box::new(move |a, b| !ids.contains(&a) && !ids.contains(&b))
    }

    fn ignore_one(flake: String) -> Box<Fn(String, String) -> bool> {
        let flakes = vec![flake.to_string()].into_iter().collect();
        ignore(flakes)
    }

    #[test]
    fn leader_elected () {
        let _ = env_logger::try_init();
        let a: String = "a".to_owned();
        let b: String = "b".to_owned();
        let c: String = "c".to_owned();
        let ids: Vec<&String> = vec![&a, &b, &c];

        {
            let switch = Switchboard::new(ids);

            for _ in 0..100 {
                switch.tick();
                switch.process_all_messages();
            }

            assert!(switch.leader() != None);
        }
    }

    #[test]
    fn leader_stability () {
        let _ = env_logger::try_init();
        let a: String = "a".to_owned();
        let b: String = "b".to_owned();
        let c: String = "c".to_owned();
        let ids: Vec<&String> = vec![&a, &b, &c];

        {
            let switch = Switchboard::new(ids.clone());

            let mut rng = thread_rng();
            let mut flake = a.clone();

            let mut stable_ticks = 0;
            for tick in 0..1000 {
                if tick % 50 == 0 {
                    flake = rng.choose(&ids).unwrap().to_string();
                    trace!("disconnecting {}", flake);
                }
                switch.tick();
                switch.process_messages(&ignore_one(flake.clone()));

                let valid: Vec<_> = switch.leaders().into_iter().filter(|l|
                    l.to_string() != flake
                ).collect();
                assert!(valid.len() <= 1);
                if valid.len() == 1 {
                    stable_ticks += 1;
                }
            }

            trace!("stable for {}", stable_ticks);
            assert!(stable_ticks > 900);
        }
    }

    #[test]
    fn cluster_follows () {
        let _ = env_logger::try_init();
        let a: String = "a".to_owned();
        let b: String = "b".to_owned();
        let c: String = "c".to_owned();
        let ids: Vec<&String> = vec![&a, &b, &c];

        {
            let switch = Switchboard::new(ids);

            for _ in 0..100 {
                switch.tick();
                switch.process_all_messages();
            }

            let mut final_index = 0;
            let leader_log = {
                let leader_id = switch.leader().unwrap();
                let mut leader = switch.nodes.get(leader_id).unwrap().borrow_mut();

                for i in 0..47 {
                    let committed = leader.raft.propose(Box::new(Record(i))).unwrap();
                    assert!(committed >= final_index);
                    final_index = committed;
                }

                leader.log.record_vec()
            };

            info!("proposal complete");

            for _ in 0..50 {
                switch.tick();
                switch.process_all_messages();
            }

            for cell in switch.nodes.values() {
                let node = cell.borrow();
                let log = node.log.record_vec();
                assert_eq!(leader_log, log);
                assert_eq!(*node.raft.volatile_state.commit_count, final_index + 1);
            }
        }
    }

    #[test]
    fn leader_ignores_duplicates () {
        let _ = env_logger::try_init();
        let a: String = "a".to_owned();
        let b: String = "b".to_owned();
        let c: String = "c".to_owned();
        let ids: Vec<&String> = vec![&a, &b, &c];

        {
            let switch = Switchboard::new(ids);

            for _ in 0..100 {
                switch.tick();
                switch.process_all_messages();
            }

            let mut final_index = 0;
            let leader_log = {
                let leader_id = switch.leader().unwrap();
                let mut leader = switch.nodes.get(leader_id).unwrap().borrow_mut();

                for i in 0..47 {
                    let committed = leader.raft.propose(Box::new(Record(i))).unwrap();
                    let other = leader.raft.propose(Box::new(Record(i))).unwrap();
                    assert!(committed >= final_index);
                    assert!(committed == other);
                    final_index = committed;
                }

                leader.log.record_vec()
            };

            info!("proposal complete");

            for _ in 0..50 {
                switch.tick();
                switch.process_all_messages();
            }

            for cell in switch.nodes.values() {
                let node = cell.borrow();
                let log = node.log.record_vec();
                assert_eq!(leader_log, log);
                assert_eq!(*node.raft.volatile_state.commit_count, final_index + 1);
            }
        }
    }

    #[test]
    fn unstable_cluster_progress () {
        let _ = env_logger::try_init();
        let a: String = "a".to_owned();
        let b: String = "b".to_owned();
        let c: String = "c".to_owned();
        let ids: Vec<&String> = vec![&a, &b, &c];

        {
            let mut rng = thread_rng();
            let switch = Switchboard::new(ids.clone());

            for _ in 0..100 {
                switch.tick();
                switch.process_all_messages();
            }

            let mut flake = a.clone();
            let mut final_index = 0;
            for tick in 0..1000 {
                let valid: Vec<_> = switch.leaders().into_iter().filter(|l|
                    l.to_string() != flake
                ).collect();
                assert!(valid.len() <= 1);
                if valid.len() == 1 {
                    let leader_id = valid.get(0).unwrap().to_string();
                    let mut leader = switch.nodes.get(&leader_id).unwrap().borrow_mut();

                    if tick % 100 == 42 {
                        let count = random::<u64>() % 10;
                        info!("proposing {}", count);
                        for i in 0..count {
                            let committed = leader.raft.propose(Box::new(Record(i))).unwrap();
                        }
                    }

                    let official = *leader.raft.volatile_state.commit_count;
                    trace!("official {} prior {}", official, final_index);
                    final_index = official;
                }

                if tick % 100 == 84 {
                    flake = rng.choose(&ids).unwrap().to_string();
                    trace!("disconnecting {}", flake);
                }

                switch.tick();
                switch.process_messages(&ignore_one(flake.clone()));
            }

            info!("pencils down, time to recover {}", flake);
            for _ in 0..200 {
                switch.tick();
                switch.process_all_messages();
            }

            let leader_log = {
                let leader_id = switch.leader().unwrap();
                let mut leader = switch.nodes.get(leader_id).unwrap().borrow_mut();

                leader.log.record_vec()
            };

            for cell in switch.nodes.values() {
                let node = cell.borrow();
                let log = node.log.record_vec();
                assert_eq!(leader_log, log);
            }
        }
    }

    #[test]
    fn vote_granted () {
        let _ = env_logger::try_init();
        let log: MemoryLog<Record> = MemoryLog::new();
        let link = NullLink::new();
        {
            let id = "me".to_owned();
            let cluster = single_node_cluster(&id);
            let mut raft: Raft<Record> = Raft::new(cluster, &DEFAULT_CONFIG, Box::new(log.clone()), Box::new(link));
            let response = raft.request_vote(RequestVote {
                source: "other".to_string(),
                term: 0,
                candidate_id: "george michael".to_string(),
                last_log: LogEntry { term: 0, index: 5 }
            });
            assert_eq!(response.vote_granted, true);
        }
        assert_eq!(log.get_voted_for(), Some("george michael".to_string()));
    }
}
