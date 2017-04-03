use operation::Operation;

use std::collections::HashMap;

pub enum MessageType {
    Dummy,
    UpdateOperationRequest,
    UpdateOperationResponse,
    SendMeasurementsRequest,
    SendMeasurementsResponse,
}

pub struct Message {
    message_type: MessageType,
    update_operation_request: Option<HashMap<u64, u64>>,
    update_operation_response: Option<HashMap<u64, Vec<Operation>>>,
    send_measurements_request: Option<Vec<String>>,
}

