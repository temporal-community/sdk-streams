# temporalio-streams-log

Experimental. The shared part of the Stores that keep each Stream as a log.

A `LogStore` has a few log calls: create, append, read, wait and close. This crate turns one
into a `StreamStore` of `temporalio-streams`, which answers the five operations of the Stream
service.

- The crate does what is the same for every log: the form of an offset, the digest of a batch,
  the pages and the deadline of a read, and the check that a Stream has expired.
- A `LogStore` does what must happen in one step with its write. It gives each entry its id, and
  it applies the fence of a Producer's epochs, the dedupe of a batch that comes again, and
  Retention. It also waits for a new entry. Its `Capabilities` say whether it has the fence, the
  close and the numbering of a batch with no sequence, and the crate refuses a call that needs
  one that is missing.
- A Store crate runs the shared suites, which check that its `LogStore` keeps these rules as the
  other Stores do.

`stream_runtime` gives a `StreamRuntime` on a `LogStore`. `dispatch::call` is the byte form of the
five operations, for a binding in another language. It takes an operation name and the encoded
request, and gives the encoded response. A `LazyStore` connects its inner Store at the first call
that needs it.

## The shared suites and vectors

The feature `test-support` exports the shared suites in `testing`. A Store crate implements
`testing::TestStore` for its Store and runs each suite with `testing::run`, so that every Store
keeps one set of rules. `testdata/*.json` are the shared vectors of the key layout, the meta
fields, the offsets and the batch digest. `examples/gen_vectors.rs` writes them from a second
implementation that shares no code with the crate.
