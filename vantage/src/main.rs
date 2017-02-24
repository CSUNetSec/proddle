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

use chan::Sender;
use clap::{App, ArgMatches};
use proddle::{Error, Measurement};
use threadpool::ThreadPool;

mod operation_job;
mod client;

use operation_job::OperationJob;

use std::collections::{BinaryHeap, HashMap};
use std::process::Command;
use std::sync::{Arc, RwLock};

fn parse_args<'a>(matches: &'a ArgMatches) -> Result<(String, String, String, u64, usize, String, u64, u32, HashMap<&'a str, i64>, Vec<&'a str>), Error> {
    let hostname = try!(value_t!(matches, "HOSTNAME", String));
    let ip_address = try!(value_t!(matches, "IP_ADDRESS", String));
    let measurements_directory = try!(value_t!(matches, "MEASUREMENTS_DIRECTORY", String));
    let bucket_count = try!(value_t!(matches.value_of("BUCKET_COUNT"), u64));
    let thread_count = try!(value_t!(matches.value_of("THREAD_COUNT"), usize));
    let bridge_ip_address = try!(matches.value_of("BRIDGE_IP_ADDRESS").ok_or("failed to parse bridge ip address"));
    let bridge_port = try!(value_t!(matches.value_of("BRIDGE_PORT"), u16));
    let bridge_address = format!("{}:{}", bridge_ip_address, bridge_port);
    let bridge_update_interval_seconds = try!(value_t!(matches.value_of("BRIDGE_UPDATE_INTERVAL_SECONDS"), u64));
    let send_results_interval_seconds = try!(value_t!(matches.value_of("SEND_RESULTS_INTERVAL_SECONDS"), u32));
    let include_tags = match matches.values_of("INCLUDE_TAGS") {
        Some(include_tags) => {
            let mut hash_map = HashMap::new();
            for include_tag in include_tags {
                let mut split_values = include_tag.split("|");
                let tag = try!(split_values.nth(0).ok_or("failed to fetch include tag"));
                let interval = try!(try!(split_values.nth(0).ok_or("failed to fetch include tag interval")).parse::<i64>());

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

    Ok((hostname, ip_address, measurements_directory, bucket_count, thread_count, bridge_address, 
        bridge_update_interval_seconds, send_results_interval_seconds, include_tags, exclude_tags))
}

pub fn main() {
    env_logger::init().unwrap();
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();
    
    //initialize vantage parameters
    info!("parsing command line arguments");
    let (hostname, ip_address, measurements_directory, bucket_count, thread_count, bridge_address, 
         bridge_update_interval_seconds, send_results_interval_seconds, include_tags, exclude_tags) = match parse_args(&matches) {
        Ok(args) => args,
        Err(e) => panic!("{}", e),
    };

    //initialize vantage data structures
    info!("initializing vantage data structures");
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

    //start recv result channel
    let (tx, rx) = chan::sync(0);
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
                                if let Err(e) = execute_measurement(pool_operation_job, &pool_hostname, 
                                        &pool_ip_address, &pool_measurements_directory, pool_tx) {
                                    error!("{}", e);
                                }
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
        info!("retrieving updates from bridge");
        match client::update_measurements(measurements.clone(), &measurements_directory, &bridge_address) {
            Ok(measurements_added) => info!("added {} measurements", measurements_added),
            Err(e) => error!("{}", e),
        }

        match client::update_operations(operations.clone(), operation_bucket_hashes.clone(), &include_tags, &exclude_tags, &bridge_address) {
            Ok(operations_added) => info!("added {} operations", operations_added),
            Err(e) => error!("{}", e),
        }

        std::thread::sleep(std::time::Duration::new(bridge_update_interval_seconds, 0))
    }
}

fn execute_measurement(operation_job: OperationJob, hostname: &str, ip_address: &str, measurements_directory: &str, tx: Sender<String>) -> Result<(), Error> {
    //execute operation and store results in json string
    let mut result = format!("{{\"timestamp\":{},\"hostname\":\"{}\",\"ip_address\":\"{}\",\"measurement\":\"{}\",\"domain\":\"{}\",\"url\":\"{}\"",
            time::now_utc().to_timespec().sec,
            hostname,
            ip_address,
            operation_job.operation.measurement,
            operation_job.operation.domain,
            operation_job.operation.url);

    let mut arguments = Vec::new();
    if let Some(parameters) = operation_job.operation.parameters {
        for (key, value) in parameters.iter() {
            arguments.push(format!("--{}=\"{}\"", key, value));
        }
    }

    match Command::new("python")
                .arg(format!("{}/{}", measurements_directory, operation_job.operation.measurement))
                .arg(operation_job.operation.url)
                .args(&arguments)
                .output() {
        Ok(output) => {
            let stderr= String::from_utf8_lossy(&output.stderr);
            match stderr.len() {
                0 => result.push_str(&format!(",\"error\":false,\"result\":{}", String::from_utf8_lossy(&output.stdout))),
                _ => result.push_str(&format!(",\"error\":true,\"error_message\":\"{}\"", stderr)),
            }
        },
        Err(e) => result.push_str(&format!(",\"error\":true,\"error_message\":\"{}\"", e)),
    };

    result.push_str("}");

    tx.send(result);
    Ok(())
}
