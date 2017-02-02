extern crate env_logger;
extern crate capnp;
extern crate capnp_rpc;
#[macro_use]
extern crate chan;
#[macro_use]
extern crate clap;
extern crate futures;
#[macro_use]
extern crate log;
extern crate proddle;
extern crate threadpool;
extern crate time;
extern crate tokio_core;

use clap::App;
use proddle::Measurement;
use threadpool::ThreadPool;

mod operation_job;
mod client;

use operation_job::OperationJob;

use std::collections::{BinaryHeap, HashMap};
use std::process::Command;
use std::sync::{Arc, RwLock};

pub fn main() {
    env_logger::init().unwrap();
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();

    //initialize vantage parameters
    info!("parsing command line arguments");
    let hostname = matches.value_of("HOSTNAME").unwrap().to_owned();
    let ip_address = matches.value_of("IP_ADDRESS").unwrap().to_owned();
    let measurements_directory = matches.value_of("MEASUREMENTS_DIRECTORY").unwrap().to_owned();
    let bucket_count = match matches.value_of("BUCKET_COUNT").unwrap().parse::<u64>() {
        Ok(bucket_count) => bucket_count,
        Err(e) => panic!("failed to parse bucket_count as u64: {}", e),
    };

    let thread_count = match matches.value_of("THREAD_COUNT").unwrap().parse::<usize>() {
        Ok(thread_count) => thread_count,
        Err(e) => panic!("failed to parse thread_count as usize: {}", e),
    };

    let bridge_address = &format!("{}:{}", matches.value_of("BRIDGE_IP_ADDRESS").unwrap(), matches.value_of("BRIDGE_PORT").unwrap());
    let bridge_update_interval_seconds = match matches.value_of("BRIDGE_UPDATE_INTERVAL_SECONDS").unwrap().parse::<u64>() {
        Ok(bridge_update_interval_seconds) => bridge_update_interval_seconds,
        Err(e) => panic!("failed to parse bridge_update_interval_seconds as u64: {}", e),
    };

    let send_results_interval_seconds = match matches.value_of("SEND_RESULTS_INTERVAL_SECONDS").unwrap().parse::<u32>() {
        Ok(send_results_interval_seconds) => send_results_interval_seconds,
        Err(e) => panic!("failed to parse send_results_interval_seconds as u32: {}", e),
    };

    let include_tags = match matches.values_of("INCLUDE_TAGS") {
        Some(include_tags) => {
            let mut hash_map = HashMap::new();
            for include_tag in include_tags {
                let mut split_values = include_tag.split("|");
                let tag = match split_values.nth(0) {
                    Some(tag) => tag,
                    None => panic!("failed to collect include tag name"),
                };

                let interval = match split_values.nth(0) {
                    Some(interval) => {
                        match interval.parse::<i64>() {
                            Ok(interval) => interval,
                            Err(e) => panic!("failed to parse include tag interval as u32: {}", e),
                        }
                    },
                    None => panic!("failed to collect include tag interval"),
                };

                hash_map.insert(tag, interval);
            }

            hash_map
        },
        None => HashMap::new(),
    };

    let exclude_tags = match matches.values_of("EXCLUDE_TAGS") {
        Some(exclude_tags) => exclude_tags.collect(),
        None => Vec::new(),
    };

    info!("initializing vantage data structures");

    //initialize vantage data structures
    let measurements: Arc<RwLock<HashMap<String, Measurement>>> = Arc::new(RwLock::new(HashMap::new()));
    let operations: Arc<RwLock<HashMap<u64, BinaryHeap<OperationJob>>>> = Arc::new(RwLock::new(HashMap::new()));
    let operation_bucket_hashes: Arc<RwLock<HashMap<u64, u64>>> = Arc::new(RwLock::new(HashMap::new()));

    //populate operations with buckets
    {
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

    //create result channels
    let (tx, rx) = chan::sync(0);

    info!("service started");

    //start recv result channel
    let thread_bridge_address = bridge_address.clone();
    std::thread::spawn(move || {
        let mut result_buffer: Vec<String> = Vec::new();
        let tick = chan::tick_ms(send_results_interval_seconds * 1000);

        loop {
            chan_select! {
                rx.recv() -> result => {
                    match result {
                        Some(result) => result_buffer.push(result),
                        None => error!("failed to retrieve result from channel"),
                    }
                },
                tick.recv() => {
                    if result_buffer.len() > 0 {
                        info!("sending {} results to bridge", result_buffer.len());
                        if let Err(e) = client::send_results(&mut result_buffer, &thread_bridge_address) {
                            error!("failed to send results: {}", e);
                        };
                    }
                },
            }
        }
    });

    //start thread for scheduling operations
    let thread_operations = operations.clone();
    let thread_measurements_directory = measurements_directory.to_owned();
    std::thread::spawn(move || {
        let thread_pool = ThreadPool::new(thread_count);

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

                        //if the next execution time is earlier then the current time then execute
                        if execution_time < now {
                            let mut operation_job = operation_jobs.pop().unwrap();
                            let pool_operation_job = operation_job.clone();
                            operation_job.execution_time += operation_job.interval;
                            operation_jobs.push(operation_job);

                            //add job to thread pool
                            let (pool_tx, pool_hostname, pool_ip_address, pool_measurements_directory) =
                                (tx.clone(), hostname.clone(), ip_address.clone(), thread_measurements_directory.clone());

                            thread_pool.execute(move || {
                                //execute operation and store results in json string
                                let mut result = format!("{{\"timestamp\":{},\"hostname\":\"{}\",\"ip_address\":\"{}\",\"measurement\":\"{}\",\"domain\":\"{}\",\"url\":\"{}\"",
                                        time::now_utc().to_timespec().sec,
                                        pool_hostname,
                                        pool_ip_address,
                                        pool_operation_job.operation.measurement,
                                        pool_operation_job.operation.domain,
                                        pool_operation_job.operation.url);

                                let mut arguments = Vec::new();
                                if let Some(parameters) = pool_operation_job.operation.parameters {
                                    for (key, value) in parameters.iter() {
                                        arguments.push(format!("--{}=\"{}\"", key, value));
                                    }
                                }

                                match Command::new("python")
                                            .arg(format!("{}/{}", pool_measurements_directory, pool_operation_job.operation.measurement))
                                            .arg(pool_operation_job.operation.url)
                                            .args(&arguments)
                                            .output() {
                                    Ok(output) => result.push_str(&format!(",\"error\":false,\"result\":{}", String::from_utf8_lossy(&output.stdout))),
                                    Err(e) => result.push_str(&format!(",\"error\":true,\"error_message\":\"{}\"", e)),
                                };

                                result.push_str("}");

                                //send result string over result channel
                                pool_tx.send(result);
                            });
                        } else {
                            break;
                        }
                    }
                }
            }

            std::thread::sleep(std::time::Duration::new(5, 0));
        }
    });

    //start loop to periodically request measurements and operations
    loop {
        info!("polling bridge");
        if let Err(e) = client::poll_bridge(measurements.clone(), &measurements_directory, operations.clone(), operation_bucket_hashes.clone(), &include_tags, &exclude_tags, bridge_address) {
            error!("failed to poll bridge: {}", e);
        }

        std::thread::sleep(std::time::Duration::new(bridge_update_interval_seconds, 0))
    }
}
