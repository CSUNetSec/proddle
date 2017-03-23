use bson::Bson;
use chan::{self, Sender};
use proddle::ProddleError;
use rand::{self, Rng};
use serde_json;
use time;

use measurement;
use operation_job::OperationJob;

use std;
use std::process::Command;

pub struct Executor {
    operation_tx: Sender<OperationJob>,
}

impl Executor {
    pub fn new(thread_count: usize, hostname: &str, ip_address: &str, max_retries: i32, measurement_tx: Sender<Bson>) -> Executor {
        let (operation_tx, operation_rx) = chan::sync(0);
        for _ in 0..thread_count {
            let thread_operation_rx = operation_rx.clone();
            let (t_hostname, t_ip_address, t_measurement_tx) = (hostname.to_owned(), ip_address.to_owned(), measurement_tx.clone());
            let _ = std::thread::spawn(move || {
                loop {
                    chan_select! {
                        thread_operation_rx.recv() -> operation_job => {
                            match operation_job {
                                Some(operation_job) => {
                                    if let Err(e) = execute_measurement(operation_job, &t_hostname, &t_ip_address, max_retries, t_measurement_tx.clone()) {
                                        error!("{}", e);
                                    }
                                },
                                None => {
                                    warn!("executor thread recv 'None' operation job");
                                },
                            }
                        },
                    }
                }
            });
        }

        Executor {
            operation_tx: operation_tx,
        }
    }

    pub fn execute_operation(&mut self, operation_job: OperationJob) -> Result<(), ProddleError> {
        self.operation_tx.send(operation_job);
        Ok(())
    }
}

fn execute_measurement(operation_job: OperationJob, hostname: &str, ip_address: &str, max_retries: i32, tx: Sender<Bson>) -> Result<(), ProddleError> {
    //TODO create measurement arguments
    /*let mut arguments = Vec::new();
    if let Some(parameters) = operation_job.operation.parameters {
        for parameter in parameters {
            arguments.push(format!("--{}=\"{}\"", parameter.name, parameter.value));
        }
    }*/

    for i in 0..max_retries {
        //execute measurement
        let timestamp = time::now_utc().to_timespec().sec;
        let measurement_result = match operation_job.operation.measurement.as_ref() {
            "HttpGet" => measurement::http_get::execute(&operation_job.operation.domain),
            _ => return Err(ProddleError::from(format!("Unknown measurement class '{}'.", operation_job.operation.measurement))),
        };

        //parse result
        let mut document = match measurement_result {
            Ok(Bson::Document(document)) => document,
            Err(e) => {
                error!("{}", e); //unknown error
                break;
            },
            _ => return Err(ProddleError::from("Failed to parse measurement result as Bson::Document")),
        };

        document.insert_bson(String::from("timestamp"), bson!(timestamp));
        document.insert_bson(String::from("vantage_hostname"), bson!(hostname));
        document.insert_bson(String::from("vantage_ip_address"), bson!(ip_address));
        document.insert_bson(String::from("measurement_class"), bson!(&operation_job.operation.measurement));
        document.insert_bson(String::from("measurement_domain"), bson!(&operation_job.operation.domain));

        //check for errors and handle if necessary
        if document.contains_key("internal_error_message") {
            //if internal error - send document and break
            tx.send(Bson::Document(document));
            break;
        } else if document.contains_key("measurement_error_message") {
            //if measurement error - send document and try again
            document.insert_bson(String::from("remaining_attempts"), bson!(max_retries - 1 - i));
            tx.send(Bson::Document(document));
        } else {
            //if no error - send document and break
            tx.send(Bson::Document(document));
            break;
        }

        std::thread::sleep(std::time::Duration::new(rand::thread_rng().gen_range(10, 20), 0))
    }

    Ok(())
}

/*fn execute_measurement(operation_job: OperationJob, hostname: &str, ip_address: &str, measurements_directory: &str, max_retries: u8, tx: Sender<String>) -> Result<(), ProddleError> {
    //create measurement arguments
    let mut arguments = Vec::new();
    if let Some(parameters) = operation_job.operation.parameters {
        for parameter in parameters {
            arguments.push(format!("--{}=\"{}\"", parameter.name, parameter.value));
        }
    }

    //execute measurement
    let common_fields = format!("\"hostname\":\"{}\",\"ip_address\":\"{}\",\"measurement\":\"{}\",\"domain\":\"{}\",\"url\":\"{}\"",
            hostname,
            ip_address,
            operation_job.operation.measurement,
            operation_job.operation.domain,
            operation_job.operation.url);

    for i in 0..max_retries {
        let timestamp = time::now_utc().to_timespec().sec;
        let measurement_output = Command::new("python")
                .arg(format!("{}/{}", measurements_directory, operation_job.operation.measurement))
                .arg(&operation_job.operation.url)
                .args(&arguments)
                .output();

        //gather measurement output
        let (internal_error, output_fields) = match measurement_output {
            Ok(output) => {
                match output.stderr.len() {
                    0 => (false, format!("\"error\":false,\"result\":{}", String::from_utf8_lossy(&output.stdout))),
                    _ => (true, format!("\"error\":true,\"error_message\":\"{}\"", String::from_utf8_lossy(&output.stderr))),
                }
            },
            Err(e) => (true, format!("\"error\":true,\"error_message\":\"{}\"", e)),
        };
        
        //parse json document
        let json_string = format!("{{\"timestamp\":{},\"remaining_attempts\":{},{},{}}}", timestamp, max_retries - 1 - i, common_fields, output_fields);
        let json = match serde_json::from_str(&json_string) {
            Ok(json) => json,
            Err(e) => {
                error!("failed to parse json string '{}': {}", json_string, e);
                continue;
            },
        };

        //check if retry required
        tx.send(json_string);
        if internal_error {
            break; //internal error (no retry)
        } else {
            if let Bson::Document(document) = Bson::from_json(&json) {
                if let Some(&Bson::Document(ref result_document)) = document.get("result") {
                    if let Some(&Bson::Boolean(false)) = result_document.get("error") {
                        break; //no error in measurement (no retry)
                    }
                }
            }
        }

        std::thread::sleep(std::time::Duration::new(rand::thread_rng().gen_range(10, 20), 0))
    }

    Ok(())
}*/
