use bincode::{self, Infinite};
use bytes::{BigEndian, Buf, BufMut, BytesMut};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Decoder, Encoder, Framed};
use tokio_proto::pipeline::{ClientProto, ServerProto};

use error::ProddleError;
use operation::Operation;

use std;
use std::collections::HashMap;
use std::io::Cursor;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum MessageType {
    Dummy,
    UpdateOperationsRequest,
    UpdateOperationsResponse,
    SendMeasurementsRequest,
    SendMeasurementsResponse,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub message_type: MessageType,
    pub update_operations_request: Option<HashMap<u64, u64>>,
    pub update_operations_response: Option<HashMap<u64, Vec<Operation>>>,
    pub send_measurements_request: Option<Vec<String>>,
    pub send_measurements_response: Option<Vec<bool>>,
}

impl Message {
    pub fn new_update_operations_request() -> Message {
        Message {
            message_type: MessageType::UpdateOperationsRequest,
            update_operations_request: None,
            update_operations_response: None,
            send_measurements_request: None,
            send_measurements_response: None,
        }
    }
}

pub struct MessageCodec;

impl Encoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn encode(&mut self, msg: Message, buf: &mut BytesMut) -> std::io::Result<()> {
        let encoded: Vec<u8> = bincode::serialize(&msg, Infinite).unwrap();
        buf.put_u32::<BigEndian>(4 + encoded.len() as u32);
        buf.put_slice(&encoded);
        Ok(())
    }
}

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Message>> {
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

pub struct ProddleProto;

impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for ProddleProto {
    type Request = Message;
    type Response = Message;
    type Transport = Framed<T, MessageCodec>;
    type BindTransport = Result<Self::Transport, std::io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MessageCodec))
    }
}

impl<T: AsyncRead + AsyncWrite + 'static> ClientProto<T> for ProddleProto {
    type Request = Message;
    type Response = Message;
    type Transport = Framed<T, MessageCodec>;
    type BindTransport = Result<Self::Transport, std::io::Error>;

    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MessageCodec))
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
