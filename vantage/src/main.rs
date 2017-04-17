#[macro_use(bson, doc)]
extern crate bson;
#[macro_use]
extern crate chan;
#[macro_use]
extern crate clap;
extern crate curl;
extern crate proddle;
extern crate rand;
#[macro_use]
extern crate slog;
#[macro_use]
extern crate slog_scope;
extern crate slog_term;
extern crate time;

use bson::Document;
use clap::{App, ArgMatches};
use proddle::ProddleError;
use slog::{DrainExt, Logger};

mod client;
mod executor;
mod measurement;
mod operation_job;

use client::Client;
use executor::Executor;
use operation_job::OperationJob;

use std::collections::{BinaryHeap, HashMap};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};

fn parse_args<'a>(matches: &'a ArgMatches) -> Result<(String, String, u64, usize, SocketAddr, u32, i32, u32, HashMap<&'a str, i64>, Vec<&'a str>), ProddleError> {
    let hostname = try!(value_t!(matches, "HOSTNAME", String));
    let ip_address = try!(value_t!(matches, "IP_ADDRESS", String));
    let bucket_count = try!(value_t!(matches.value_of("BUCKET_COUNT"), u64));
    let thread_count = try!(value_t!(matches.value_of("THREAD_COUNT"), usize));
    let bridge_ip_address = try!(matches.value_of("BRIDGE_IP_ADDRESS").ok_or("failed to parse bridge ip address"));
    let bridge_port = try!(value_t!(matches.value_of("BRIDGE_PORT"), u16));
    let bridge_address = try!(SocketAddr::from_str(&format!("{}:{}", bridge_ip_address, bridge_port)));
    let bridge_update_interval_seconds = try!(value_t!(matches.value_of("BRIDGE_UPDATE_INTERVAL_SECONDS"), u32));
    let max_retries = try!(value_t!(matches.value_of("MAX_RETRIES"), i32));
    let send_measurements_interval_seconds = try!(value_t!(matches.value_of("SEND_MEASUREMENTS_INTERVAL_SECONDS"), u32));
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

    Ok((hostname, ip_address, bucket_count, thread_count, bridge_address, bridge_update_interval_seconds, 
        max_retries, send_measurements_interval_seconds, include_tags, exclude_tags))
}

pub fn main() {
    slog_scope::set_global_logger(Logger::root(slog_term::streamer().build().fuse(), o![]));
    let yaml = load_yaml!("args.yaml");
    let matches = App::from_yaml(yaml).get_matches();
    
    //initialize vantage parameters
    info!("parsing command line arguments");
    let (hostname, ip_address, bucket_count, thread_count, socket_addr, bridge_update_interval_seconds, 
            max_retries, send_measurements_interval_seconds, include_tags, exclude_tags) = match parse_args(&matches) {
        Ok(args) => args,
        Err(e) => panic!("{}", e),
    };

    //initialize vantage data structures
    info!("initializing vantage data structures");
    let mut operations: HashMap<u64, BinaryHeap<OperationJob>> = HashMap::new();
    let mut operation_bucket_hashes: HashMap<u64, u64> = HashMap::new();
    let client = Arc::new(RwLock::new(Client::new(socket_addr.clone())));

    //populate operations with buckets
    let mut counter = 0;
    let delta = u64::max_value() / bucket_count;
    for _ in 0..bucket_count {
        operations.insert(counter, BinaryHeap::new());
        operation_bucket_hashes.insert(counter, 0);
        counter += delta;
    }

    //initialize operations
    {
        let mut client = client.write().unwrap();
        match client.update_operations(&mut operations, &mut operation_bucket_hashes, &include_tags, &exclude_tags) {
            Ok(updated_operations_count) => {
                if updated_operations_count > 0 {
                    info!("updated {} operation(s)", updated_operations_count);
                }
            },
            Err(e) => error!("{}", e),
        }
    }

    //start recv measurement channel
    let (measurement_tx, measurement_rx) = chan::sync(50);
    let t_client = client.clone();
    std::thread::spawn(move || {
        let mut measurement_buffer: Vec<Document> = Vec::new();
        let tick = chan::tick_ms(send_measurements_interval_seconds * 1000);

        loop {
            chan_select! {
                measurement_rx.recv() -> measurement => {
                    match measurement {
                        Some(measurement) => measurement_buffer.push(measurement),
                        None => error!("failed to retrieve measurement from channel"),
                    }
                },
                tick.recv() => {
                    if measurement_buffer.len() > 0 {
                        info!("sending {} measurements to bridge", measurement_buffer.len());
                        let mut client = t_client.write().unwrap();
                        if let Err(e) = client.send_measurements(&mut measurement_buffer) {
                            error!("failed to send measurements: {}", e);
                        };
                    }
                },
            }
        }
    });

    //start operation loop
    let mut executor = Executor::new(thread_count, &hostname, &ip_address, max_retries, measurement_tx);

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
                let mut client = client.write().unwrap();
                match client.update_operations(&mut operations, &mut operation_bucket_hashes, &include_tags, &exclude_tags) {
                    Ok(updated_operations_count) => {
                        if updated_operations_count > 0 {
                            info!("updated {} operation(s)", updated_operations_count);
                        }
                    },
                    Err(e) => error!("{}", e),
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
