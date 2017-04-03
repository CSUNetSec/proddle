use bytes::{BigEndian, Buf, BufMut, BytesMut};
use tokio_io::codec::{Encoder, Decoder};

use error::ProddleError;
use operation::Operation;

use std::collections::HashMap;
use std::io::Cursor;

pub enum MessageType {
    Dummy,
    UpdateOperationsRequest,
    UpdateOperationsResponse,
    SendMeasurementsRequest,
    SendMeasurementsResponse,
}

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
        let mut msg_buf: Vec<u8> = Vec::new();

        //encode message type
        match msg.message_type {
            MessageType::UpdateOperationsRequest => msg_buf.push(0u8),
            MessageType::UpdateOperationsResponse => msg_buf.push(1u8),
            MessageType::SendMeasurementsRequest => msg_buf.push(2u8),
            MessageType::SendMeasurementsResponse => msg_buf.push(3u8),
            _ => buf.put(255u8),
        }

        buf.put_u32::<BigEndian>(4 + msg_buf.len() as u32);
        buf.put_slice(&msg_buf);
        println!("encoded length: {}", buf.len());
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

        //decode message type
        let message_type = match buf[4] {
            0 => MessageType::UpdateOperationsRequest,
            1 => MessageType::UpdateOperationsResponse,
            2 => MessageType::SendMeasurementsRequest,
            3 => MessageType::SendMeasurementsResponse,
            _ => MessageType::Dummy,
        };

        Ok(
            Some(
                Message{
                    message_type: message_type,
                    update_operations_request: None,
                    update_operations_response: None,
                    send_measurements_request: None,
                    send_measurements_response: None,
                }
            )
        )
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
