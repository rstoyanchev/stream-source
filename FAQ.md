# FAQ

- [Why does Source not implement Iterator and Autocloseable?](#why-does-source-not-implement-iterator-and-autocloseable)
- [Why receiveNext and not requestNext?](#why-receivenext-and-not-requestnext)
- [Is this push, pull, or hybrid style?](#is-stream-source-push-pull-or-hybrid-style-and-in-that-regard-how-does-it-compare-to-javautilstreamstream-and-reactive-streams)
- [Should a producer implement Source or Producer?](#should-a-producer-implement-source-or-producer)


### Why does `Source` not implement `Iterator` and `Autocloseable`?
  
`Iterator` and `Source` are very different contracts.
One operates on a local collection, the other fetches from a remote source.
`hasNext` is a simple local check, `receiveNext` performs I/O and can throw `IOException` and `InterruptedException`.
  
The different semantics are reflected in the names.
`hasNext` implies a simple check that is not possible for a `Source` without fetching.
`receiveNext` makes it clear it receives and blocks to do that.
  
The main goal is to emulate the simplicity of using an `Iterator`.
The alternative would be a loop where each iteration is guarded
with `isClosed()` and `null` checks at a minimum.

### Why `receiveNext` and not `requestNext`?

While "request" is familiar if coming from the Reactive Streams world,
it does not accurately reflect what's actually happening.
We are not making a request for an item to be delivered later.
We are actually receiving and blocking until the item is ready.

### Is *stream-source* push, pull, or hybrid style, and in that regard how does it compare to `java.util.stream.Stream` and Reactive Streams?

Push-style refers to a producer pushing directly to a consumer as soon as items become available (async callback handling).
The consumer remains passive, waiting for events or payloads to be delivered to it.
No latency, but the producer can overwhelm the consumer.

Pull-style refers to a consumer explicitly pulling from the producer when ready.
The producer remains passive until called, and the consumer is never overwhelmed,
but there is extra latency when the producer is passive.

Reactive Streams is hybrid-style that combines pull-based flow control with push-based delivery
to solve the fast-producer/slow-consumer problem without sacrificing latency.

`java.util.stream.Stream` is push-style, internal pipeline methods pass items through callbacks.
However, it is not very relevant as it operates on finite collections that will not overwhelm the consumer.

`stream-source` is always pull-style for consumers.
Producers can be pull-style (`Source` implementation) or push-style (`Producer`),
but the use of `BlockingQueue` and the blocking semantics of the `Sink` contract
provide natural backpressure between an independent producer and consumer.

This can be described as pull-style with an optional push-style `Producer` that is also set up
in support of pull-style consumption through the use of a `BlockingQueue` for backpressure.

### Should a producer implement `Source` or `Producer`?

You can use either. There are some differences to keep in mind.

From a code perspective, `Producer` can feel more convenient.
It puts the producer in control of the producing loop.
By comparison, `Source` produces only when called, one item at a time.

`Producer` is well suited to be delared in a lambda.

For this convenience `Producer` needs an `Exchange` and a separate thread to operate independently of the consumer loop.
By comparison, `Source` is more flexible and can either be pulled from directly (in the same thread) or adapted to `Producer` and pulled indirectly from through an `Exchange`.

`Source` is well suited for higher-level transformations and having additional, generic capabilities through transparent decoration.
