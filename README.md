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
- try again if failure
- error analyzer to look at historical data as well (if there is a success within 5 minutes of a failure call it a success

- fix error handling in measurement/operation
- add timestamp to info logging
- validate hostname and ip address on vantage
- validate module scripts before they're uploaded
- use pip3 to install dependencies of modules
