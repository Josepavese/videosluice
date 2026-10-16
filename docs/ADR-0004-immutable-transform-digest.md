# ADR 0004: reuse immutable spatial-transform digests at the source boundary

A native capture incident attributed a 3.829-second wall interval (1.306 ms
thread CPU) to output transform metadata, before authority, layout and bounded
RAM submission. The transform's SHA256 was recomputed for every analysis frame,
although its seven geometry inputs were unchanged. This identifies an avoidable
hot-path dependency; it does not prove whether scheduling, crypto-provider
internals or storage-backed page waits caused that interval, nor whether this
optimization alone prevents every capture interruption.

Keep one fixed, capture-thread-owned digest entry in each native source object.
Reuse requires exact source width/height/stride, destination width/height,
orientation, aspect policy and hash-provider/context equality. On a miss,
invalidate first and run the unchanged canonical encoder/hash; publish the cache
only on success. A source object cannot reopen after close. Alternating layouts
remain correct with bounded recomputation rather than an expanding cache.

The cache contains no pixels, frame references, epochs, authority decisions or
payload hashes. Those remain independently validated for every publication.
There is no wire/API change, new thread, lock, allocation, deadline or fallback.
Moving hashing to a new queue or changing its backend would introduce unrelated
ownership and qualification requirements for invariant metadata and is rejected.

Sanitized native tests compare the unchanged digest, exercise every key and
provider change, reject invalid inputs/failures without stale output, and prove
one hash for repeated geometry. The actual native mailbox fixture also requires
19 hits/one miss with twenty distinct frame payloads and unchanged origins.
Integrated hardware stability remains a separate qualification, not a fixture
claim.
