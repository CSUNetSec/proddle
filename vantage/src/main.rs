#[macro_use(bson, doc)]
extern crate bson;
extern crate capnp;
extern crate capnp_rpc;
#[macro_use]
extern crate chan;
#[macro_use]
extern crate clap;
extern crate curl;
extern crate futures;
extern crate proddle;
extern crate rand;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate slog_term;
extern crate serde_json;
extern crate time;
extern crate tokio_core;

use clap::{App, ArgMatches};
use proddle::{ProddleError, Measurement};
use slog::{DrainExt, Logger};

mod client;
mod executor;
mod measurement;
mod operation_job;

use executor::Executor;
use operation_job::OperationJob;

use std::collections::{BinaryHeap, HashMap};

fn parse_args<'a>(matches: &'a ArgMatches) -> Result<(String, String, String, u64, usize, String, u32, u8, u32, HashMap<&'a str, i64>, Vec<&'a str>), ProddleError> {
    let hostname = try!(value_t!(matches, "HOSTNAME", String));
    let ip_address = try!(value_t!(matches, "IP_ADDRESS", String));
    let measurements_directory = try!(value_t!(matches, "MEASUREMENTS_DIRECTORY", String));
    let bucket_count = try!(value_t!(matches.value_of("BUCKET_COUNT"), u64));
    let thread_count = try!(value_t!(matches.value_of("THREAD_COUNT"), usize));
    let bridge_ip_address = try!(matches.value_of("BRIDGE_IP_ADDRESS").ok_or("failed to parse bridge ip address"));
    let bridge_port = try!(value_t!(matches.value_of("BRIDGE_PORT"), u16));
    let bridge_address = format!("{}:{}", bridge_ip_address, bridge_port);
    let bridge_update_interval_seconds = try!(value_t!(matches.value_of("BRIDGE_UPDATE_INTERVAL_SECONDS"), u32));
    let max_retries = try!(value_t!(matches.value_of("MAX_RETRIES"), u8));
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
        bridge_update_interval_seconds, max_retries, send_results_interval_seconds, include_tags, exclude_tags))
}

pub fn main() {
    slog_scope::set_global_logger(Logger::root(slog_term::streamer().build().fuse(), o![]));
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();
    
    //initialize vantage parameters
    info!("parsing command line arguments");
    let (hostname, ip_address, measurements_directory, bucket_count, thread_count, bridge_address, 
         bridge_update_interval_seconds, max_retries, send_results_interval_seconds, include_tags, exclude_tags) = match parse_args(&matches) {
        Ok(args) => args,
        Err(e) => panic!("{}", e),
    };

    //initialize vantage data structures
    info!("initializing vantage data structures");
    let mut measurements: HashMap<String, Measurement> = HashMap::new();
    let mut operations: HashMap<u64, BinaryHeap<OperationJob>> = HashMap::new();
    let mut operation_bucket_hashes: HashMap<u64, u64> = HashMap::new();

    //populate operations with buckets
    let mut counter = 0;
    let delta = u64::max_value() / bucket_count;
    for _ in 0..bucket_count {
        operations.insert(counter, BinaryHeap::new());
        operation_bucket_hashes.insert(counter, 0);
        counter += delta;
    }

    //initialize measurements and operations
    if let Err(e) = get_bridge_updates(&mut measurements, &mut operations, &mut operation_bucket_hashes, 
            &include_tags, &exclude_tags, &measurements_directory, &bridge_address) {
        panic!("{}", e);
    }

    //start recv result channel
    let (result_tx, result_rx) = chan::sync(50);
    let thread_bridge_address = bridge_address.clone();
    std::thread::spawn(move || {
        let mut result_buffer: Vec<String> = Vec::new();
        let tick = chan::tick_ms(send_results_interval_seconds * 1000);

        loop {
            chan_select! {
                result_rx.recv() -> result => {
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

    //start operation loop
    let mut executor = Executor::new(thread_count, &hostname, &ip_address, &measurements_directory, max_retries, result_tx);

    let execute_operations_tick = chan::tick_ms(5 * 1000);
    let bridge_update_tick = chan::tick_ms(bridge_update_interval_seconds * 1000);
    loop {
        chan_select! {
            execute_operations_tick.recv() => {
                if let Err(e) = execute_operations(&mut operations, &mut executor) {
                    error!("{}", e);
                }
            },
            bridge_update_tick.recv() => {
                if let Err(e) = get_bridge_updates(&mut measurements, &mut operations, &mut operation_bucket_hashes, 
                        &include_tags, &exclude_tags, &measurements_directory, &bridge_address) {
                    error!("{}", e);
                }
            }
        }
    }
}

fn execute_operations(operations: &mut HashMap<u64, BinaryHeap<OperationJob>>, executor: &mut Executor) -> Result<(), ProddleError> {
    let now = time::now_utc().to_timespec().sec;

    //iterate over buckets of operation jobs
    for (_, operation_jobs) in operations.iter_mut() {
        loop {
            let execution_time = match operation_jobs.peek() {
                Some(operation_job) => operation_job.execution_time,
                None => break,
            };

            //if the next execution time is earlier then the current time then execute
            if execution_time < now {
                let mut operation_job = match operation_jobs.pop() {
                    Some(operation_job) => operation_job,
                    None => {
                        warn!("execute_operations() 'pop' a 'None' value from operation_jobs");
                        continue;
                    },
                };

                let pool_operation_job = operation_job.clone();
                operation_job.execution_time += operation_job.interval;
                operation_jobs.push(operation_job);

                //add job to executor
                let _ = executor.execute_operation(pool_operation_job);
            } else {
                break;
            }
        }
    }

    Ok(())
}

fn get_bridge_updates(measurements: &mut HashMap<String, Measurement>, operations: &mut HashMap<u64, BinaryHeap<OperationJob>>, 
        operation_bucket_hashes: &mut HashMap<u64, u64>, include_tags: &HashMap<&str, i64>, exclude_tags: &Vec<&str>, 
        measurements_directory: &str, bridge_address: &str) -> Result<(), ProddleError> {
    let measurements_updated = try!(client::update_measurements(measurements, measurements_directory, bridge_address)); 
    if measurements_updated > 0 {
        info!("updated {} measurement(s)", measurements_updated);
    }

    let operations_updated = try!(client::update_operations(operations, operation_bucket_hashes, include_tags, exclude_tags, bridge_address));
    if operations_updated > 0 {
        info!("updated {} operation(s)", operations_updated);
    }

    Ok(())
}
