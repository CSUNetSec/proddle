use bson::{Bson, Document};
use curl::easy::{Easy, List};

use proddle::ProddleError;

use std::collections::HashMap;
use std::time::Duration;

static PREFIXES: [&'static str; 2] = ["", "www."];

pub fn execute(domain: &str, parameters: &HashMap<String, String>) -> Result<Document, ProddleError> {
    let mut easy: Option<Easy> = None;
    let (mut internal_error_message, mut measurement_error_message) = (None, None);
    let mut headers = Vec::new();
    let mut content = Vec::new();

    //parse parameters
    let timeout = match parameters.get("timeout") {
        Some(timeout) => try!(timeout.parse()),
        None => 30,
    };

    //iterate over prefixes and attempt each one
    for prefix in PREFIXES.iter() {
        easy = None;
        internal_error_message = None;
        measurement_error_message = None;
        headers.clear();
        content.clear();

        match send_request(&format!("{}{}", prefix, domain), timeout, &mut headers, &mut content) {
            Ok((e, error_message)) => {
                //check for measurement error
                easy = Some(e);
                match error_message {
                    Some(_) => measurement_error_message = error_message,
                    None => break,
                }

                continue;
            },
            Err(e) => {
                //internal_error
                internal_error_message = Some(format!("{}", e));
                continue;
            }
        }
    }

    //create Bson object for measurement, populating as many fields as possible
    let mut document = doc!();
    if let Some(internal_error_message) = internal_error_message {
        document.insert_bson(String::from("internal_error_message"), bson!(internal_error_message));
    }

    if let Some(measurement_error_message) = measurement_error_message {
        document.insert_bson(String::from("measurement_error_message"), bson!(measurement_error_message));
    }

    if let Some(mut easy) = easy {
        if let Ok(response_code) = easy.response_code() {
            document.insert_bson(String::from("response_code"), bson!(response_code));
        }

        let headers = headers.iter().map(|x| Bson::String(x.to_owned())).collect();
        document.insert_bson(String::from("headers"), Bson::Array(headers));
        document.insert_bson(String::from("content_size"), bson!(content.len() as i32));

        if let Ok(total_time) = easy.total_time() {
            document.insert_bson(String::from("total_time"), bson!(parse_time(&total_time)));
        }

        if let Ok(namelookup_time) = easy.namelookup_time() {
            document.insert_bson(String::from("name_lookup_time"), bson!(parse_time(&namelookup_time)));
        }

        if let Ok(connect_time) = easy.connect_time() {
            document.insert_bson(String::from("connect_time"), bson!(parse_time(&connect_time)));
        }

        if let Ok(appconnect_time) = easy.appconnect_time() {
            document.insert_bson(String::from("app_connect_time"), bson!(parse_time(&appconnect_time)));
        }

        if let Ok(pretransfer_time) = easy.pretransfer_time() {
            document.insert_bson(String::from("pre_transfer_time"), bson!(parse_time(&pretransfer_time)));
        }

        if let Ok(starttransfer_time) = easy.starttransfer_time() {
            document.insert_bson(String::from("start_transfer_time"), bson!(parse_time(&starttransfer_time)));
        }

        if let Ok(redirect_count) = easy.redirect_count() {
            document.insert_bson(String::from("redirect_count"), bson!(redirect_count));
        }

        if let Ok(redirect_time) = easy.redirect_time() {
            document.insert_bson(String::from("redirect_time"), bson!(parse_time(&redirect_time)));
        }

        if let Ok(Some(effective_url)) = easy.effective_url() {
            document.insert_bson(String::from("effective_url"), bson!(effective_url));
        }

        if let Ok(Some(primary_ip)) = easy.primary_ip() {
            document.insert_bson(String::from("primary_ip_address"), bson!(primary_ip));
        }

        if let Ok(primary_port) = easy.primary_port() {
            document.insert_bson(String::from("primary_port"), bson!(primary_port as i32));
        }
    }

    Ok(document)
}

fn parse_time(duration: &Duration) -> f64 {
    duration.as_secs() as f64 + (duration.subsec_nanos() as f64 / 1000000000.0)
}

fn send_request(url: &str, timeout: u64, headers: &mut Vec<String>, content: &mut Vec<u8>) -> Result<(Easy, Option<String>), ProddleError> {
    let mut error_message = None;
    let mut easy = Easy::new();
    try!(easy.url(url));
    try!(easy.get(true));
    try!(easy.timeout(Duration::new(timeout, 0))); //30 second timeout
    try!(easy.follow_location(true)); //follow redirects
    try!(easy.http_transfer_decoding(true)); //request compressed http response
    try!(easy.accept_encoding("")); //accept all supported encodings
    try!(easy.useragent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.110 Safari/537.36"));

    //set http headers
    let mut list = List::new();
    try!(list.append("Upgrade-Insecure-Requests: 1"));
    try!(list.append("Connection: keep-alive"));
    //try!(list.append("Accept-Language: en-US,en;q=0.8"));
    try!(easy.http_headers(list));
    
    //set data transfer function
    {
        let mut transfer = easy.transfer();

        try!(transfer.header_function(|header| {
            headers.push(String::from_utf8_lossy(header).into_owned().replace("\r\n", ""));
            true
        }));

        try!(transfer.write_function(|data| {
            content.extend_from_slice(data);
            Ok(data.len())
        }));

        if let Err(e) = transfer.perform() {
            error_message = Some(format!("{}", e));
        }
    }

    Ok((easy, error_message))
}
