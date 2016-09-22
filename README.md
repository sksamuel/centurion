akka-patterns
=============

[![Build Status](https://travis-ci.org/sksamuel/akka-patterns.png)](https://travis-ci.org/sksamuel/akka-patterns)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.akka/akka-patterns_2.10*.svg?label=latest%20release%20for%202.11"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22avro4s-core_2.11%22)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.akka/akka-patterns_2.11*.svg?label=latest%20release%20for%202.12"/>](http://search.maven.org/#search%7Cga%7C1%7Cavro4s-core_2.12)

This project provides implementations of some common enterprise integration patterns and other more general
patterns for Akka. Many of these patterns are straightforward to implement and certainly not rocket science, but
are not provided out of the box in akka. Therefore it's useful to have a single library that is well tested
and saves each project re-inventing the wheel. Contributions welcome.

## Patterns

#### Enveloping Actor

The EnvelopingActor will wrap any incoming messages in an Envelope object, together with attributes
describing that message. This pattern is used when you need to associate metadata with the message.
The typical example would be when a correlation id is needed for a message exchange.

#### Countdown Latch

The CountdownLatch will buffer incoming messages until a predefined limit has been reached.
Once the limit is reached it will then dispatch the buffered messages and any further messages will
be sent as normal. This can be thought of as the actor equivalent of the Java CountdownLatch concurrency primitive.

#### Grouping Actor

The GroupingActor will buffer incoming messages into lists of a predefined size. Once the required number of messages
has been received then those messages will be sent to the target actor as an array of messages. This is useful when
you want to process messages in batches.

#### Timeout Flow Control Actor

The TimeoutFlowControlActor is an actor that will send messages to a target actor while ensuring that the number of
outstanding (yet to be acknowledged messages) does not exceed a set threshold. Each time an ack is received the next
buffered message is sent. If no ack is received within a set period of time then that message is considered lost
and the next message is sent. This actor is similar to how TCP flow control works except without the reliability side.

#### Reliable Flow Control Actor

The ReliableFlowControlActor will ensure delivery of messages to a target actor while ensuring that the number of
outstanding (yet to be acknowledged messages) does not exceed a set threshold. Each time an ack is received the next
buffered message is sent. If no ack is received within a user defined duration then the message is resent.
This actor is similar to how TCP flow control works.

#### Acknowledging Actor

The AcknowledgingActor is an actor that will send an ack to the sender as soon as a message is received.
This actor is most often used as the other end to the flow control actors.

#### Aggregator

The Aggregator will combine messages with the same correlation id and then dispatch as an array to the target.
The aggregating actor is created with the types of messages that are required before a "complete" message is ready.

#### Periodic Actor

The PeriodicActor broadcasts tick messages at a user defined interval interleaved with the normal messages it receives.
These tick messages can be used by an implementing actor to perform logic based on durations.

#### Timeout Actor

The TimeoutActor will terminate itself if a message is not received within a user defined duration. This actor
is useful for implementing session time out patterns.

#### Keep Alive Actor

The KeepAliveActor will send a heartbeat after a user defined duration has passed without another message being
received. Each time a message is received then the heartbeat countdown is reset. This actor is analogous to the
keep-alive pings you would see in a TCP connection.

#### Pausable Actor

The PausableActor is a finite state machine with two states - paused or running.
If paused then any messages are buffered until the actor is resumed. If the actor is running then all messages
are forwarded as normal to the target actor.

#### Splitter

The Splitter accepts collections of messages and dispatches them singularly to a target actor. The actor understands
messages of type Iterable, Iterator, Array, Java Iterables and Java Iterators.

#### Resequencer

The Resequencer receives out of sequence messages and publishes them in order. The actor uses a Sequence Id
on the message envelope to order the messages. If a message is received out of order it is buffered until the
expected message arrives.

#### Wiretap

#### Discarding Throttling Actor

The DiscardingThrottlingActor is a rate limiting actor that will send messages with a minimum defined interval.
Any messages received during this minimum interval will be discarded, with the exception of the most recent,
which will be sent once the interval has expired. This pattern is useful for cases such as FX quotes
where only the most recent is required and the receiver can be overloaded (hence the need for throttling).

#### Buffering Throttling Actor

The BufferingThrottlingActor is a rate limiting actor that will send messages with a minimum defined interval.
Any messages received during this minimum interval will be buffered and replayed at the defined interval.
This pattern is useful when an erratic incoming stream is required to be converted to a consistent stream.

#### Dynamic Router

#### Discarding Barrier

The DiscardingBarrier accepts a collection of message types and waits until at least one message of each
specified type has been received. While blocked any messages received are discarded. Once all message types have been
received, then the barrier is unlocked and then it will forward all future messages to the target actor.

#### Buffering Barrier

The BufferingBarrier accepts a collection of message types and waits until at least one message of each
specified type has been received. While blocked any messages received are blocked. Once all message types have been
received, then the barrier is unlocked and then it will forward all buffered and future messages to the target actor.

#### Workpile Actor

The WorkpileMaster and WorkpileWorker specify a way of using worker-pull semantics. When a worker starts it requests
work from the master. If work is available it is sent a packet of work and once complete will request another. If
no work is currently available the master will queue all workers until work arrives. If when work arrives there are
no free workers then the master will queue the work until the next worker is available.

## Mailboxes

#### Lifo Mailbox

The LifoMailbox processes messages last in first out.

#### Priority Mailbox

The PriorityMailbox processes messages in a priority queue. The messages must be of type Envelope with an attached
attribute for the priority. If the priority is not specified then it is assumed to be of least importance and will
be processed after all pending messages that have a defined priority.

#### Expiring Mailbox

The ExpiringMailbox processes messages first in first out but with an additional timeout per message.
If the message is not processed before the timeout for that message then it is discarded.

## Routers

#### One Time Router

The OneTimeRouter routes messages to routees that are created for the processing of that single message.
After processing that message they are then terminated via a poison pill message.

## How to use

To use in an SBT project add the dependency:

```scala
libraryDependencies += "com.sksamuel.akka" % "akka-patterns_2.11" % "VERSION"
```

And Maven users should add:

```xml
<dependency>
    <groupId>com.sksamuel.akka</groupId>
    <artifactId>akka-patterns_2.11</artifactId>
    <version>VERISON</version>
</dependency>
```

You can always find the latest version using the links at the top.

## License
```
This software is licensed under the Apache 2 license, quoted below.

Copyright 2013 Stephen Samuel

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
