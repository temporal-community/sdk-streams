# temporalio-streams-memory

Experimental. A Stream Store that keeps its Streams in the memory of one process, for tests. It
is a `LogStore` of `temporalio-streams-log`. It keeps the key layout of the Redis Store and
applies the same rules in the same order.
