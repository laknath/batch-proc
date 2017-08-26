### A reliable batch processor for Mongo ##

This library is a helper for processing batches in Mongo reliably.

#### Features

* Fetch records with a defined query (
* Update records “processing”


#### TODO

* Distributed locking to support multiple daemons
* Send records to the channel
* Update “processed”
* Timeout to update “Processing” back to “initial”
* Option to opt in/out ME when fetching batches
* Option to update records to "processed" by batch, time interval or rolling
* Error channel
