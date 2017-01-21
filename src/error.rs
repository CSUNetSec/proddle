extern crate capnp;

use std;
use std::fmt::{Display, Formatter, Result};

#[derive(Debug)]
pub enum Error {
    AddrParse(std::net::AddrParseError),
    Capnp(capnp::Error),
    Io(std::io::Error),
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter) -> Result {
        match *self {
            Error::AddrParse(ref err) => write!(f, "AddrParseError: {}", err),
            Error::Capnp(ref err) => write!(f, "CapnpError: {}", err),
            Error::Io(ref err) => write!(f, "IoError: {}", err),
        }
    }
}

impl From<std::net::AddrParseError> for Error {
    fn from(err: std::net::AddrParseError) -> Error {
        Error::AddrParse(err)
    }
}

impl From<capnp::Error> for Error {
    fn from(err: capnp::Error) -> Error {
        Error::Capnp(err)
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}
