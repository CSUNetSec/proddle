pub fn get_mongodb_client(host: &str, port: u16) -> Result<Client, ProddleError> {
    Ok(try!(Client::connect(host, port)))
}

pub fn find_measurement(db: &Database, measurement_name: &str, version: Option<i32>, order_by_version: bool) -> Result<Option<OrderedDocument>, ProddleError> {
    //create search document
    let search_document = match version {
        Some(search_version) => Some(doc!("name" => measurement_name, "version" => search_version)),
        None => Some(doc!("name" => measurement_name)),
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_version {
        true => Some(doc!("version" => negative_one)),
        false => None,
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        oplog_replay: false,
        skip: None,
        limit: Some(1),
        cursor_type: CursorType::NonTailable,
        batch_size: None,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //execute find one
    Ok(try!(db.collection("measurements").find_one(search_document, find_options)))
}

pub fn find_measurements(db: &Database, measurement_name: Option<&str>, version: Option<i32>, limit: Option<i64>, order_by_version: bool) -> Result<Cursor, ProddleError> {
    //create search document
    let search_document = match measurement_name {
        Some(search_measurement_name) => {
            match version {
                Some(search_version) => Some(doc!("name" => search_measurement_name, "version" => search_version)),
                None => Some(doc!("name" => search_measurement_name)),
            }
        },
        None => {
            match version {
                Some(search_version) => Some(doc!("version" => search_version)),
                None => None,
            }
        },
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_version {
        true => Some(doc!("version" => negative_one)),
        false => None,
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        oplog_replay: false,
        skip: None,
        limit: limit,
        cursor_type: CursorType::NonTailable,
        batch_size: None,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //execute find
    Ok(try!(db.collection("measurements").find(search_document, find_options)))
}

pub fn find_operation(db: &Database, domain: &str, measurement_name: Option<&str>, order_by_timestamp: bool) -> Result<Option<OrderedDocument>, ProddleError> {
    //create search document
    let search_document = match measurement_name {
        Some(search_measurement_name) => Some(doc!("domain" => domain, "measurement" => search_measurement_name)),
        None => Some(doc!("domain" => domain)),
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_timestamp {
        true => Some(doc!("timestamp" => negative_one)),
        false => Some(doc!("_id" => 1)),
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        oplog_replay: false,
        skip: None,
        limit: Some(1),
        cursor_type: CursorType::NonTailable,
        batch_size: None,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //execute find one
    Ok(try!(db.collection("operations").find_one(search_document, find_options)))
}

pub fn find_operations(db: &Database, domain: Option<&str>, measurement_name: Option<&str>, limit: Option<i64>, order_by_timestamp: bool) -> Result<Cursor, ProddleError> {
    //create search document
    let search_document = match domain {
        Some(domain) => {
            match measurement_name {
                Some(search_measurement_name) => Some(doc!("domain" => domain, "measurement" => search_measurement_name)),
                None => Some(doc!("domain" => domain)),
            }
        },
        None => {
            match measurement_name {
                Some(search_measurement_name) => Some(doc!("measurement" => search_measurement_name)),
                None => None,
            }
        }
    };

    //create find options
    let negative_one = -1;
    let sort_document = match order_by_timestamp {
        true => Some(doc!("timestamp" => negative_one)),
        false => Some(doc!("_id" => 1)),
    };

    let find_options = Some(FindOptions {
        allow_partial_results: false,
        no_cursor_timeout: false,
        oplog_replay: false,
        skip: None,
        limit: limit,
        cursor_type: CursorType::NonTailable,
        batch_size: None,
        comment: None,
        max_time_ms: None,
        modifiers: None,
        projection: None,
        sort: sort_document,
        read_preference: None,
    });

    //specify operations collection
    Ok(try!(db.collection("operations").find(search_document, find_options)))
}
