use bincode;
use bson;
use clap;
use curl;
use mongodb;

use std;
use std::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum ProddleError {
    AddrParse(std::net::AddrParseError),
    Bincode(Box<bincode::ErrorKind>),
    Clap(clap::Error),
    Curl(curl::Error),
    DecoderError(bson::DecoderError),
    EncoderError(bson::EncoderError),
    Io(std::io::Error),
    MongoDB(mongodb::Error),
    ParseIntError(std::num::ParseIntError),
    Proddle(String),
}

impl Display for ProddleError {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match *self {
            ProddleError::AddrParse(ref err) => write!(f, "AddrParseError: {}", err),
            ProddleError::Bincode(ref err) => write!(f, "Bincode: {}", err),
            ProddleError::Clap(ref err) => write!(f, "ClapError: {}", err),
            ProddleError::Curl(ref err) => write!(f, "CurlError: {}", err),
            ProddleError::DecoderError(ref err) => write!(f, "DecoderError: {}", err),
            ProddleError::EncoderError(ref err) => write!(f, "EncoderError: {}", err),
            ProddleError::Io(ref err) => write!(f, "IoError: {}", err),
            ProddleError::MongoDB(ref err) => write!(f, "MongoDBError: {}", err),
            ProddleError::ParseIntError(ref err) => write!(f, "ParseIntError: {}", err),
            ProddleError::Proddle(ref err) => write!(f, "ProddleError: {}", err),
        }
    }
}

impl From<std::net::AddrParseError> for ProddleError {
    fn from(err: std::net::AddrParseError) -> ProddleError {
        ProddleError::AddrParse(err)
    }
}

impl From<Box<bincode::ErrorKind>> for ProddleError {
    fn from(err: Box<bincode::ErrorKind>) -> ProddleError {
        ProddleError::Bincode(err)
    }
}

impl From<clap::Error> for ProddleError {
    fn from(err: clap::Error) -> ProddleError {
        ProddleError::Clap(err)
    }
}

impl From<curl::Error> for ProddleError {
    fn from(err: curl::Error) -> ProddleError {
        ProddleError::Curl(err)
    }
}

impl From<bson::DecoderError> for ProddleError {
    fn from(err: bson::DecoderError) -> ProddleError {
        ProddleError::DecoderError(err)
    }
}

impl From<bson::EncoderError> for ProddleError {
    fn from(err: bson::EncoderError) -> ProddleError {
        ProddleError::EncoderError(err)
    }
}

impl From<std::io::Error> for ProddleError {
    fn from(err: std::io::Error) -> ProddleError {
        ProddleError::Io(err)
    }
}

impl From<mongodb::Error> for ProddleError {
    fn from(err: mongodb::Error) -> ProddleError {
        ProddleError::MongoDB(err)
    }
}

impl From<std::num::ParseIntError> for ProddleError {
    fn from(err: std::num::ParseIntError) -> ProddleError {
        ProddleError::ParseIntError(err)
    }
}

impl<'a> From<&'a str> for ProddleError {
    fn from(err: &'a str) -> ProddleError {
        ProddleError::Proddle(String::from(err))
    }
}

impl From<String> for ProddleError {
    fn from(err: String) -> ProddleError {
        ProddleError::Proddle(err)
    }
}
