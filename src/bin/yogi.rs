extern crate docopt;
extern crate rustc_serialize;

use docopt::Docopt;

const USAGE: &'static str = "
yogi

USAGE:
    yogi (-h | --help)
    yogi cancel <module-name> <domain>
    yogi download <local-filename> <module-name>
    yogi schedule <module-name> <domain>
    yogi search (--module-name=<module-name> | --domain=<domain>)
    yogi update <local-filename> <module-name>
    yogi upload <local-filename> <module-name>

OPTIONS:
    -h --help                       Display this screen.
    --domain=<domain>               Domain to perform operation.
    --module-name=<module-name>     Name of module to perform operation.
";

#[derive(Debug, RustcDecodable)]
struct Args {
    cmd_cancel: bool,
    cmd_download: bool,
    cmd_schedule: bool,
    cmd_search: bool,
    cmd_update: bool,
    cmd_upload: bool,
    arg_domain: String,
    arg_local_filename: String,
    arg_module_name: String,
}

fn main() {
    let args: Args = Docopt::new(USAGE)
                        .and_then(|d| d.decode())
                        .unwrap_or_else(|e| e.exit());

    if args.cmd_cancel {
        unimplemented!();
    } else if args.cmd_download {
        unimplemented!();
    } else if args.cmd_schedule {
        unimplemented!();
    } else if args.cmd_search {
        unimplemented!();
    } else if args.cmd_update {
        unimplemented!();
    } else if args.cmd_upload {
        unimplemented!();
    }
}
