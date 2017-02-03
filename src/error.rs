extern crate capnp;
extern crate clap;
extern crate mongodb;

use std;
use std::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum Error {
    AddrParse(std::net::AddrParseError),
    ParseIntError(std::num::ParseIntError),
    Capnp(capnp::Error),
    Clap(clap::Error),
    Io(std::io::Error),
    MongoDB(mongodb::Error),
    Proddle(String),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match *self {
            Error::AddrParse(ref err) => write!(f, "AddrParseError: {}", err),
            Error::ParseIntError(ref err) => write!(f, "ParseIntError: {}", err),
            Error::Capnp(ref err) => write!(f, "CapnpError: {}", err),
            Error::Clap(ref err) => write!(f, "ClapError: {}", err),
            Error::Io(ref err) => write!(f, "IoError: {}", err),
            Error::MongoDB(ref err) => write!(f, "MongoDBError: {}", err),
            Error::Proddle(ref err) => write!(f, "ProddleError: {}", err),
        }
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Error {
        Error::AddrParse(err)
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(err: std::num::ParseIntError) -> Error {
        Error::ParseIntError(err)
    }
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Error {
        Error::Capnp(err)
    }
}

impl From<clap::Error> for Error {
    fn from(err: clap::Error) -> Error {
        Error::Clap(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<mongodb::Error> for Error {
    fn from(err: mongodb::Error) -> Error {
        Error::MongoDB(err)
    }
}

impl<'a> From<&'a str> for Error {
    fn from(err: &'a str) -> Error {
        Error::Proddle(String::from(err))
    }
}

impl From<String> for Error {
    fn from(err: String) -> Error {
        Error::Proddle(err)
    }
}
