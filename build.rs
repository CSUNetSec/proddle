extern crate capnpc;

fn main() {
    capnpc::CompilerCommand::new().file("capnproto/proddle.capnp").src_prefix("capnproto").run().unwrap();
}
