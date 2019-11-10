use crate::{ClusterConfig, Log, LogData, Unique};

use std::cell::RefCell;
use std::cmp::min;
use std::fmt::Debug;
use std::rc::Rc;

pub struct State<Record> {
    pub term: u64,
    pub voted_for: Option<String>,
    pub records: Vec<(u64, Box<LogData<Record>>)>
}

#[derive(Clone)]
pub struct MemoryLog<Record> {
    pub state: Rc<RefCell<State<Record>>>
}

fn unboxed<T: Clone> (records: &Vec<(u64, Box<T>)>) -> Vec<(u64, Box<T>)> {
    records.iter().map(|&(t, ref v)| (t, (*v).clone())).collect()
}

impl<Record: Clone + Unique> MemoryLog<Record> {
    pub fn new () -> Self {
        let state = State { term: 0, voted_for: None, records: vec![] };
        MemoryLog {
            state: Rc::new(RefCell::new(state))
        }
    }

    pub fn data_vec(&self) -> Vec<(u64, Box<LogData<Record>>)> {
        let ref records = self.state.borrow().records;
        unboxed(records)
    }

    pub fn record_vec(&self) -> Vec<(u64, Record)> {
        self.data_vec().into_iter().flat_map(|(ix, data)| {
            match *data {
                LogData::Entry(r) => Some((ix, r)),
                _ => None
            }
        }).collect()
    }
}

impl<Record: Clone + Debug + Unique> Log<Record> for MemoryLog<Record> {
    fn get_current_term (&self) -> u64 {
        self.state.borrow().term
    }

    fn set_current_term (&mut self, term: u64) {
        self.state.borrow_mut().term = term;
    }

    fn get_voted_for (&self) -> Option<String> {
        self.state.borrow().voted_for.clone()
    }

    fn set_voted_for (&mut self, candidate: Option<String>) {
        self.state.borrow_mut().voted_for = candidate
    }

    fn get_count (&self) -> u64 {
        self.state.borrow().records.len() as u64
    }

    fn get_entry (&self, index: u64) -> Option<(u64, Box<LogData<Record>>)> {
        if index < self.get_count() {
            Some(self.state.borrow().records[index as usize].clone())
        } else {
            None
        }
    }

    fn insert (&mut self, index: u64, update: Vec<(u64, Box<LogData<Record>>)>) {
        let ref mut records = self.state.borrow_mut().records;
        records.truncate(index as usize);
        records.extend(update);
    }

    fn get_latest_config (&self) -> Option<ClusterConfig> {
        self.state.borrow().records.iter().rev().flat_map(|(_term, d)| {
            match **d {
                LogData::ClusterChange(ref cluster) => Some(cluster.clone()),
                _ => None
            }
        }).next()
    }

    fn lookup_id (&self, id: &String) -> Option<u64> {
        self.state.borrow().records.iter().position(|(_term, d)| {
            match **d {
                LogData::Entry(ref r) => r.id() == *id,
                _ => false
            }
        }).map(|v| v as u64)
    }

    fn get_batch (&self, index: u64) -> Vec<(u64, Box<LogData<Record>>)> {
        let ref records = self.state.borrow().records;
        let start = min(index as usize, records.len());
        let end = min((start + 5) as usize, records.len());
        records[start..end].to_vec()
    }
}
