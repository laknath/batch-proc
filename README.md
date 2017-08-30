### A reliable batch processor for Mongo ##

This library is a helper for processing batches in Mongo reliably.

#### Features

* Fetch records with a defined query (
* Update records “processing”
* Update “processed”
* Timeout to update “Processing” back to “initial”
* Option to update records to "processed" by batch, time interval or rolling


#### TODO

* Reset the timer in update batch when the batch is updated by exceeding min records.
* Verify objects receied before appending to the slice
* Change BufferBatch to accept struct pointer instead of a slice
* Copy the results array before streaming
* Cope when mongo connection interrupts 

* Distributed locking to support multiple daemons
* Send records to the channel
* Option to opt in/out ME when fetching batches
* Error channel
