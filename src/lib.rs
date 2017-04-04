extern crate bincode;
#[macro_use(bson, doc)]
extern crate bson;
extern crate bytes;
extern crate clap;
extern crate mongodb;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate tokio_io;
extern crate tokio_proto;

use bincode::Infinite;
use bytes::{BigEndian, Buf, BufMut, BytesMut};
use bson::ordered::OrderedDocument;

mod error;
mod message;
mod operation;

pub use self::error::ProddleError;
pub use self::message::{Message, ProddleProto};
pub use self::operation::Operation;

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::io::{Cursor, Read, Write};
use std::net::TcpStream;

pub fn message_to_stream(message: &Message, stream: &mut TcpStream) -> Result<(), ProddleError> {
    let encoded: Vec<u8> = bincode::serialize(message, Infinite).unwrap();
    let length = 4 + encoded.len() as u32;

    try!(stream.write(&[(length as u8), ((length >> 8) as u8), ((length >> 16) as u8), ((length >> 24) as u8)]));
    try!(stream.write_all(&encoded));
    try!(stream.flush());

    Ok(())
}

pub fn message_from_stream(buf: &mut Vec<u8>, stream: &mut TcpStream) -> Result<Option<Message>, ProddleError> {
    //read from stream
    let mut bytes_read = 0;
    while bytes_read < 4 {
        bytes_read += try!(stream.read(&mut buf[0..4]));
    }

    //decode length
    let length = ((buf[0] as u32) | ((buf[1] as u32) << 8) | ((buf[2] as u32) << 16) | ((buf[3] as u32) << 24)) as usize;
    while bytes_read < length {
        bytes_read += try!(stream.read(&mut buf[bytes_read..length]));
    }

    let message = bincode::deserialize(&buf[4..bytes_read]).unwrap();
    Ok(Some(message))
}

/*
 * Miscellaneous
 */
pub fn hash_string(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub fn get_bucket_key(map: &BTreeMap<u64, DefaultHasher>, key: u64) -> Option<u64> {
    let mut bucket_key = 0;
    for map_key in map.keys() {
        if *map_key > key {
            break;
        }

        bucket_key = *map_key;
    }

    Some(bucket_key)
}
