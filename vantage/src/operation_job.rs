extern crate proddle;
extern crate time;

use proddle::Operation;

use std::cmp::{Ordering, PartialOrd};

#[derive(Clone)]
pub struct OperationJob  {
    pub execution_time: i64,
    pub operation: Operation,
    pub interval: i64,
}

impl OperationJob {
    pub fn new(operation: Operation, interval: i64) -> OperationJob {
        let now = time::now_utc().to_timespec().sec;

        OperationJob {
            execution_time: (now - (now % interval) + interval),
            operation: operation,
            interval: interval,
        }
    }
}

impl PartialEq for OperationJob {
    fn eq(&self, other: &OperationJob) -> bool {
        self.execution_time == other.execution_time
    }
}

impl Eq for OperationJob {}

impl Ord for OperationJob {
    fn cmp(&self, other: &OperationJob) -> Ordering {
        other.execution_time.cmp(&self.execution_time)
    }
}

impl PartialOrd for OperationJob {
    fn partial_cmp(&self, other: &OperationJob) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
