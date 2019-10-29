use crate::{Log, Unique};

use std::cell::RefCell;
use std::cmp::min;
use std::fmt::Debug;
use std::rc::Rc;

pub struct State<Record> {
    pub term: u64,
    pub voted_for: Option<String>,
    pub records: Vec<(u64, Box<Record>)>
}

#[derive(Clone)]
pub struct MemoryLog<Record> {
    pub state: Rc<RefCell<State<Record>>>
}

fn unboxed<T: Clone> (records: &Vec<(u64, Box<T>)>) -> Vec<(u64, T)> {
    records.iter().map(|&(t, ref v)| (t, *v.clone())).collect()
}

impl<Record: Clone> MemoryLog<Record> {
    pub fn new () -> Self {
        let state = State { term: 0, voted_for: None, records: vec![] };
        MemoryLog {
            state: Rc::new(RefCell::new(state))
        }
    }

    pub fn record_vec(&self) -> Vec<(u64, Record)> {
        let ref records = self.state.borrow().records;
        unboxed(records)
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

    fn get_entry (&self, index: u64) -> Option<(u64, Box<Record>)> {
        if index < self.get_count() {
            Some(self.state.borrow().records[index as usize].clone())
        } else {
            None
        }
    }

    fn insert (&mut self, index: u64, update: Vec<(u64, Box<Record>)>) {
        let ref mut records = self.state.borrow_mut().records;
        records.truncate(index as usize);
        records.extend(update);
    }

    fn get_batch (&self, index: u64) -> Vec<(u64, Box<Record>)> {
        let ref records = self.state.borrow().records;
        let start = min(index as usize, records.len());
        let end = min((start + 5) as usize, records.len());
        records[start..end].to_vec()
    }
}
