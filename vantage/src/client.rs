use capnp::capability::Promise;
use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use futures::Future;
use proddle::{Error, Measurement, Operation};
use proddle::proddle_capnp::proddle::Client;
use tokio_core::io::Io;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;

use operation_job::OperationJob;

use std;
use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

pub fn send_results(result_buffer: &mut Vec<String>, bridge_address: &str) -> Result<(), Error> {
    //open stream
    let mut core = try!(Core::new());
    let handle = core.handle();
                        
    let socket_addr = try!(SocketAddr::from_str(bridge_address));
    let stream = try!(core.run(TcpStream::connect(&socket_addr, &handle)));

    try!(stream.set_nodelay(true));
    let (reader, writer) = stream.split();

    let network = Box::new(VatNetwork::new(reader, writer, Side::Client, Default::default()));
    let mut rpc_system = RpcSystem::new(network, None);
    let proddle: Client = rpc_system.bootstrap(Side::Server);
    handle.spawn(rpc_system.map_err(|e| error!("{:?}", e)));

    //initialize request
    let mut request = proddle.send_results_request();
    {
        let mut request_results = request.get().init_results(result_buffer.len() as u32);
        for (i, result) in result_buffer.iter().enumerate() {
            let mut request_result = request_results.borrow().get(i as u32);
            request_result.set_json_string(result);
        }
    }

    //send request and read response
    try!(
        core.run(request.send().promise.and_then(|_| {
            Promise::ok(())
        }))
    );

    //clear result buffer
    result_buffer.clear();
    Ok(())
}

pub fn poll_bridge(
        measurements: Arc<RwLock<HashMap<String, Measurement>>>,
        measurements_directory: &str,
        operations: Arc<RwLock<HashMap<u64, BinaryHeap<OperationJob>>>>,
        operation_bucket_hashes: Arc<RwLock<HashMap<u64, u64>>>,
        include_tags: &HashMap<&str, i64>,
        exclude_tags: &Vec<&str>,
        bridge_address: &str) -> Result<(), Error> {
    //open stream
    let mut core = try!(Core::new());
    let handle = core.handle();
                        
    let socket_addr = try!(SocketAddr::from_str(bridge_address));
    let stream = try!(core.run(TcpStream::connect(&socket_addr, &handle)));

    try!(stream.set_nodelay(true));
    let (reader, writer) = stream.split();

    let network = Box::new(VatNetwork::new(reader, writer, Side::Client, Default::default()));
    let mut rpc_system = RpcSystem::new(network, None);
    let proddle: Client = rpc_system.bootstrap(Side::Server);
    handle.spawn(rpc_system.map_err(|e| error!("{:?}", e)));

    //populate get measurements request
    let mut request = proddle.get_measurements_request();
    {
        let measurements = measurements.read().unwrap();
        let mut request_measurements = request.get().init_measurements(measurements.len() as u32);
        for (i, measurement) in measurements.values().enumerate() {
            let mut request_measurement = request_measurements.borrow().get(i as u32);

            if let Some(timestamp) = measurement.timestamp {
                request_measurement.set_timestamp(timestamp);
            }

            request_measurement.set_name(&measurement.name);
            request_measurement.set_version(measurement.version);
        }
    }

    //send get measurements request
    let mut measurements_added = 0;
    let response = try!(core.run(request.send().promise));
    {
        let result_measurements = try!(try!(response.get()).get_measurements());
        
        let mut measurements = measurements.write().unwrap();
        for result_measurement in result_measurements.iter() {
            let measurement = try!(Measurement::from_capnproto(&result_measurement));
            if measurement.version == 0 {
                //delete file
                try!(std::fs::remove_file(format!("{}/{}", measurements_directory, measurement.name)));

                //remove from measurements data structures
                measurements.remove(&measurement.name);
            } else {
                //create file
                let mut file = try!(File::create(format!("{}/{}", measurements_directory, measurement.name)));

                let content = measurement.content.clone().unwrap().into_bytes();
                try!(file.write_all(&content));
                try!(file.flush());

                //add to measurements data structure
                measurements.insert(measurement.name.to_owned(), measurement);
                measurements_added += 1;
            }
        }
    }

    if measurements_added > 0 {
        info!("added {} measurements", measurements_added);
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

    //send get operations request
    let mut operations_added = 0;
    let response = try!(core.run(request.send().promise));
    {
        let result_operation_buckets = try!(try!(response.get()).get_operation_buckets());

        let mut operations = operations.write().unwrap();
        let mut operation_bucket_hashes = operation_bucket_hashes.write().unwrap();
        for result_operation_bucket in result_operation_buckets.iter() {
            let mut binary_heap = BinaryHeap::new();
            let mut hasher = DefaultHasher::new();
            for result_operation in try!(result_operation_bucket.get_operations()).iter() {
                //add operation to binary heap
                let operation = try!(Operation::from_capnproto(&result_operation));
                operation.hash(&mut hasher);

                //validate tags
                let mut operation_interval = i64::max_value();
                if let Some(ref operation_tags) = operation.tags {
                    //check if tag is in exclude tags
                    let mut found = false;
                    for operation_tag in operation_tags {
                        for exclude_tag in exclude_tags {
                            if operation_tag.eq(*exclude_tag) {
                                found = true;
                            }
                        }
                    }

                    if found {
                        continue
                    }

                    //determine interval
                    for operation_tag in operation_tags {
                        for (include_tag, interval) in include_tags {
                            if operation_tag.eq(*include_tag) && *interval < operation_interval {
                                operation_interval = *interval;
                            }
                        }
                    }
                }

                //check if include tag interval was found
                if operation_interval == i64::max_value() {
                    continue;
                }

                //add operation
                binary_heap.push(OperationJob::new(operation, operation_interval));
                operations_added += 1;
            }

            //insert new operations into operations map
            operations.insert(result_operation_bucket.get_bucket(), binary_heap);
            operation_bucket_hashes.insert(result_operation_bucket.get_bucket(), hasher.finish());
        }
    }

    if operations_added > 0 {
        info!("added {} operations", operations_added);
    }

    Ok(())
}
