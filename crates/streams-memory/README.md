# temporalio-streams-memory

Experimental. A Stream Store that keeps its Streams in the memory of one process, for tests. It
is a `LogStore` of `temporalio-streams-log`. It keeps the meta fields of the Redis Store and
applies the same rules in the same order.

A Store refuses an append or a close of a Producer with a lower epoch than its newest one. The
option `fence` turns that off, with `MemoryStore::with_fence` or `MemoryStreamOptions::fence`. Then
the Store takes them, and a batch of a lower epoch has no dedupe.
