#proddle

##Overview
A distributed network measurement tool.

##Logging
We are using the env_logger crate provided by rust to perform
all logging operations. To enable logging an environment
variable "RUST_LOG" is requried to be set. To execute a
binary using this framework use the command examples.

env RUST_LOG=info ./bridge
env RUST_LOG=info ./vantage -H hostname.example.com -I 1.2.3.4

##TODO
- mass code refactor since it's now in separate projects (400 line files is unacceptable)
- fix error handling in measurement/operation
- add timestamp to info logging
- validate hostname and ip address on vantage
- validate module scripts before they're uploaded
- use pip3 to install dependencies of modules
- create docker image
