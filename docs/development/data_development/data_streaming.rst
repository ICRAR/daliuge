.. _data_streaming:

Synchronous Streaming
---------------------

This form of data streaming connects an app producer with an app consumer via the
appdrop write method and dataWritten callback method. This effectively bypasses
the datadrop component which removes the serialization performance overhead, but
does not benefit from subprocessing parallelism without data buffering within the
appdrop.

Asynchronous Streaming (Proposed)
---------------------------------

Async streaming uses async interface such that drop subprocesses
executing run() can use an async loop to await multiple io bound operations.
This has the added benefit of utilizing subprocesses and handling multiple data
streams at the cost of handling back-pressure in data drops.

Some approaches are:

asyncio and AsyncIterable - async-pull model
uses asyncio loop

ReactiveX for Python (RxPy) - async-push model
uses reactivex loop

asyncio and aioreactive - async-push and async-pull models
uses asyncio loop

Back-Pressure
"""""""""""""

Back pressure is when a stream is outputting items more rapidly than an operator
can consume them.

A cold stream can begin building back pressure until an observer sees fit to subscribe
and consume them. A cold stream will always produce the same sequence regardless of
when and how fast the observer consumes them. Synonymous to TCP.

A hot stream begins to emit immediately when it is created. A hot stream creates items
at its own pace and it's up to the observer to keep up. Synonymous to UDP.

https://reactivex.io/documentation/operators/backpressure.html

Single and Multi Stream
"""""""""""""""""""""""

AsyncMultiStream are hot

AsyncSingleStream are cold

See https://github.com/dbrattli/aioreactive