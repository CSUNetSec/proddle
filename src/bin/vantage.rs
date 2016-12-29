extern crate capnp;
extern crate capnp_rpc;
extern crate gj;
extern crate gjio;
extern crate proddle;
extern crate time;

use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use gj::EventLoop;
use proddle::{Module, Operation};
use proddle::proddle_capnp::proddle::Client;

use std::cmp::{Ordering, PartialOrd};
use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

fn main() {
    let modules: Arc<RwLock<HashMap<String, Module>>> = Arc::new(RwLock::new(HashMap::new()));
    let operations: Arc<RwLock<HashMap<u64, BinaryHeap<OperationJob>>>> = Arc::new(RwLock::new(HashMap::new()));
    let operation_bucket_hashes: Arc<RwLock<HashMap<u64, u64>>> = Arc::new(RwLock::new(HashMap::new()));

    //populate operations with buckets
    {
        let bucket_count = 10;
        let mut operations = operations.write().unwrap();
        let mut operation_bucket_hashes = operation_bucket_hashes.write().unwrap();

        let mut counter = 0;
        let delta = u64::max_value() / bucket_count;
        for _ in 0..bucket_count {
            operations.insert(counter, BinaryHeap::new());
            operation_bucket_hashes.insert(counter, 0);
            counter += delta;
        }
    }

    //start thread for scheduling operations
    let thread_operations = operations.clone();
    std::thread::spawn(move || {
        loop {
            let now = time::now_utc().to_timespec().sec;

            //iterate over buckets of operation jobs
            {
                let mut operations = thread_operations.write().unwrap();
                for (_, operation_jobs) in operations.iter_mut() {
                    loop {
                        let execution_time = match operation_jobs.peek() {
                            Some(operation_job) => operation_job.execution_time,
                            None => break,
                        };

                        if execution_time < now {
                            let mut operation_job = operation_jobs.pop().unwrap();
                            println!("EXECUTING OPERATION {} {}", operation_job.operation.domain, operation_job.operation.module);

                            operation_job.execution_time += operation_job.operation.interval as i64;
                            operation_jobs.push(operation_job);
                        } else {
                            break;
                        }
                    }
                }
            }

            std::thread::sleep(std::time::Duration::new(5, 0));
        }
    });

    //start loop to periodically request modules and operations
    loop {
        let modules = modules.clone();
        let operations = operations.clone();
        let operation_bucket_hashes = operation_bucket_hashes.clone();

        let result = EventLoop::top_level(move |wait_scope| -> Result<(), capnp::Error> {
            //open stream
            let mut event_port = try!(gjio::EventPort::new());
            let socket_addr = match SocketAddr::from_str(&format!("127.0.0.1:12289")) {
                Ok(socket_addr) => socket_addr,
                Err(e) => panic!("failed to parse socket address: {}", e),
            };

            let tcp_address = event_port.get_network().get_tcp_address(socket_addr);
            let stream = try!(tcp_address.connect().wait(wait_scope, &mut event_port));

            //connect rpc client
            let network = Box::new(VatNetwork::new(stream.clone(), stream, Side::Client, Default::default()));
            let mut rpc_system = RpcSystem::new(network, None);
            let proddle: Client = rpc_system.bootstrap(Side::Server);

            //populate get modules request
            let mut request = proddle.get_modules_request();
            {
                let modules = modules.read().unwrap();
                let mut request_modules = request.get().init_modules(modules.len() as u32);
                for (i, module) in modules.values().enumerate() {
                    let mut request_module = request_modules.borrow().get(i as u32);

                    if let Some(timestamp) = module.timestamp {
                        request_module.set_timestamp(timestamp);
                    }

                    request_module.set_name(&module.name);
                    request_module.set_version(module.version);
                }
            }

            //send modules request
            let response = try!(request.send().promise.wait(wait_scope, &mut event_port));
            let reader = try!(response.get());
            let result_modules = try!(reader.get_modules());

            //process result modules
            {
                let mut modules = modules.write().unwrap();
                for result_module in result_modules.iter() {
                    let module = match Module::from_capnproto(&result_module) {
                        Ok(module) => module,
                        Err(e) => panic!("failed to parse capnproto to module: {}", e),
                    };

                    //println!("PROCESSING MODULE {} - {}",  module.name, module.version);

                    if module.version == 0 {
                        modules.remove(&module.name);
                    } else {
                        modules.insert(module.name.to_owned(), module);
                    }
                }
            }

            //populate get operations request
            let mut request = proddle.get_operations_request();
            {
                let operation_bucket_hashes = operation_bucket_hashes.read().unwrap();
                let mut request_bucket_hashes = request.get().init_bucket_hashes(operation_bucket_hashes.len() as u32);
                for (i, (bucket_key, bucket_hash)) in operation_bucket_hashes.iter().enumerate() {
                    let mut request_bucket_hash = request_bucket_hashes.borrow().get(i as u32);

                    request_bucket_hash.set_bucket(*bucket_key);
                    request_bucket_hash.set_hash(*bucket_hash);
                }
            }

            //send operations request
            let response = try!(request.send().promise.wait(wait_scope, &mut event_port));
            let reader = try!(response.get());
            let result_operation_buckets = try!(reader.get_operation_buckets());

            //process result operations
            {
                let mut operations = operations.write().unwrap();
                let mut operation_bucket_hashes = operation_bucket_hashes.write().unwrap();
                for result_operation_bucket in result_operation_buckets.iter() {
                    let mut binary_heap = BinaryHeap::new();
                    let mut hasher = DefaultHasher::new();
                    for result_operation in try!(result_operation_bucket.get_operations()).iter() {
                        //println!("PROCESSING OPERATION {} - {}", result_operation.get_domain().unwrap(), result_operation.get_module().unwrap());

                        //add operation to binary heap
                        match Operation::from_capnproto(&result_operation) {
                            Ok(operation) => {
                                operation.hash(&mut hasher);
                                binary_heap.push(OperationJob::new(operation));
                            },
                            Err(e) => panic!("failed to parse capnproto to operation: {}", e),
                        };
                    }

                    //insert new operations into operations map
                    operations.insert(result_operation_bucket.get_bucket(), binary_heap);
                    operation_bucket_hashes.insert(result_operation_bucket.get_bucket(), hasher.finish());
                }
            }

            Ok(())
        });

        if let Err(e) = result {
            panic!("event loop failed: {}", e);
        }

        std::thread::sleep(std::time::Duration::new(1440, 0))
    }
}

/*
 * OperationJob implementation
 */
#[derive(Clone)]
struct OperationJob  {
    execution_time: i64,
    operation: Operation,
}

impl OperationJob {
    fn new(operation: Operation) -> OperationJob {
        let now = time::now_utc().to_timespec().sec;

        OperationJob {
            execution_time: (now - (now % operation.interval as i64) + operation.interval as i64),
            operation: operation,
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
