# proddle
## OVERVIEW
Proddle is a distributed network measurement tool. Additional 
documentation is available on the [official website].

#### BRIDGE
The bridge is a passive entity to obfuscate the db backend from each 
vanage. It's main purpose is to update vantage operations and write 
measurements.

#### VANTAGE
Vantages actively perform operations. For the time being we've
limited the framework to HTTP GET request, although additional
functionality is on the road map.

#### YOGI
The cli application for manual configuration.

## TODO
- validate hostname and ip address on vantage
- add information to README.md

[//]: #

   [official website]: <http://proddle.netsec.colostate.edu>
