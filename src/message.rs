use bincode::{self, Infinite};
use bytes::{BigEndian, Buf, BufMut, BytesMut};
use tokio_io::codec::{Encoder, Decoder};

use error::ProddleError;
use operation::Operation;

use std::collections::HashMap;
use std::io::Cursor;

#[derive(Clone, Deserialize, Serialize)]
pub enum MessageType {
    Dummy,
    UpdateOperationsRequest,
    UpdateOperationsResponse,
    SendMeasurementsRequest,
    SendMeasurementsResponse,
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Message {
    message_type: MessageType,
    update_operations_request: Option<HashMap<u64, u64>>,
    update_operations_response: Option<HashMap<u64, Vec<Operation>>>,
    send_measurements_request: Option<Vec<String>>,
    send_measurements_response: Option<Vec<bool>>,
}

pub struct MessageCodec;

impl Encoder for MessageCodec {
    type Item = Message;
    type Error = ProddleError;

    fn encode(&mut self, msg: Message, buf: &mut BytesMut) -> Result<(), ProddleError> {
        let encoded: Vec<u8> = bincode::serialize(&msg, Infinite).unwrap();
        buf.put_u32::<BigEndian>(4 + encoded.len() as u32);
        buf.put_slice(&encoded);
        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = ProddleError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Message>, ProddleError> {
        println!("decoded length: {}", buf.len());
        if buf.len() < 4 {
            return Ok(None);
        }

        //decode length
        let length = Cursor::new(&buf[0..4]).get_u32::<BigEndian>() as usize; 
        if buf.len() < length {
            return Ok(None);
        }
        let buf = buf.split_to(length);

        let message = bincode::deserialize(&buf[4..]).unwrap();
        Ok(Some(message))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use tokio_io::codec::{Encoder, Decoder};

    use message::{Message, MessageType, MessageCodec};

    #[test]
    fn encode_update_operations_request() {
        let message = Message {
            message_type: MessageType::UpdateOperationsRequest,
            update_operations_request: None,
            update_operations_response: None,
            send_measurements_request: None,
            send_measurements_response: None,
        };

        let mut bytes = BytesMut::with_capacity(1024);
        let mut message_codec = MessageCodec{};

        //encode message
        if let Err(_) = message_codec.encode(message, &mut bytes) {
            panic!("failed to encode UpdateOperationsRequest");
        }

        //decode message
        match message_codec.decode(&mut bytes) {
            Ok(Some(message)) => {
                match message.message_type {
                    MessageType::UpdateOperationsRequest => println!("decoded UpdateOperationsRequest"),
                    _ => panic!("failed to decode UpdateOperationsRequest message_type"),
                }
            }
            _ => panic!("failed to decode UpdateOperationsRequest"),
        }
    }
}
