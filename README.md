### A reliable batch processor for Mongo ##

This library is a helper for processing batches in Mongo reliably.

#### Features

* Fetch records with a defined query
* Update records “processing”
* Option to update records to "processed" by batch, time interval or rolling
* Support buffered streaming
* Timeout to update “Processing” back to “initial”


#### TODO

* Opting in/out time fields to store
* Cope when mongo connection interrupts 
* Distributed locking to support multiple daemons
* Option to opt in/out ME when fetching batches
* Error channel
