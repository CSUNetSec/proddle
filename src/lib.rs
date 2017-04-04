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

mod error;
mod message;
mod operation;

pub use self::error::ProddleError;
pub use self::message::{Message, ProddleProto};
pub use self::operation::Operation;

use bson::ordered::OrderedDocument;
use mongodb::{Client, ThreadedClient};
use mongodb::coll::options::{CursorType, FindOptions};
use mongodb::cursor::Cursor;
use mongodb::db::{Database, ThreadedDatabase};

use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

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
