#proddle

##Overview
A distributed network measurement tool.

##Logging
We are using the env_logger crate provided by rust to perform
all logging operations. To enable logging an environment
variable "RUST_LOG" is requried to be set. To execute a
binary using this framework use the command examples.

env RUST_LOG=info ./proddle
env RUST_LOG=info ./vantage -H hostname.example.com -I 1.2.3.4

##TODO
- add timestamp to logging
- validate hostname and ip address on vantage
- validate module scripts before they're uploaded
- use pip3 to install dependencies of modules
- create docker image

-fix error handling in measurement/operation
-change hash function in operation
-change result json fields to lowercase
-remove interval in operation (have vantage supply interval in tag)
