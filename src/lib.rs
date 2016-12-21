extern crate capnp;

pub mod proddle_capnp {
    include!(concat!(env!("OUT_DIR"), "/proddle_capnp.rs"));
}
