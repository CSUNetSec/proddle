use bson::Bson;
use capnp::capability::Promise;
use capnp_rpc::RpcSystem;
use capnp_rpc::twoparty::VatNetwork;
use capnp_rpc::rpc_twoparty_capnp::Side;
use futures::Future;
use proddle::{ProddleError, Operation};
use proddle::proddle_capnp::proddle::Client;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;
use tokio_io::AsyncRead;

use operation_job::OperationJob;

use std::collections::{BinaryHeap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::net::SocketAddr;
use std::str::FromStr;

fn open_stream(bridge_address: &str) -> Result<(Core, Client), ProddleError> {
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

    Ok((core, proddle))
}

pub fn send_measurements(measurement_buffer: &mut Vec<Bson>, bridge_address: &str) -> Result<(), ProddleError> {
    //open stream
    let (mut core, proddle) = try!(open_stream(bridge_address));

    //initialize request
    let mut request = proddle.send_measurements_request();
    {
        let mut request_measurements = request.get().init_measurements(measurement_buffer.len() as u32);
        for (i, measurement) in measurement_buffer.iter().enumerate() {
            let mut request_measurement = request_measurements.borrow().get(i as u32);
            request_measurement.set_json_string(&measurement.to_json().to_string());
        }
    }

    //send request and read response
    try!(
        core.run(request.send().promise.and_then(|_| {
            Promise::ok(())
        }))
    );

    //clear result buffer
    measurement_buffer.clear();
    Ok(())
}

pub fn update_operations(operations: &mut HashMap<u64, BinaryHeap<OperationJob>>, operation_bucket_hashes: &mut HashMap<u64, u64>,
        include_tags: &HashMap<&str, i64>, exclude_tags: &Vec<&str>, bridge_address: &str) -> Result<i32, ProddleError> {
    //open stream
    let (mut core, proddle) = try!(open_stream(bridge_address));

    //populate get operations request
    let mut request = proddle.get_operations_request();
    {
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

    Ok(operations_added)
}
