akka-patterns
=============

Implementations of EIPs using Akka


#### Release

The latest release is 0.0.1 - pending.

[![Build Status](https://travis-ci.org/sksamuel/akka-patterns.png)](https://travis-ci.org/sksamuel/akka-patterns)

### Patterns

#### Enveloping Actor

The enveloping actor will wrap any incoming messages in an Envelope object, together with attributes describing that messsage. This pattern is used when you need to associate metadata with the message. The typical example would be when a correlation id is needed for a message exchange.

#### Counting Latch

The counting latch will buffer incoming messages until a predefined limit has been reached. Once the limit is reached it will then dispatch the buffered messages and any further messages will be sent as normal.

#### Grouping Actor

The grouping actor will buffer incoming messages into lists of a predefined size. Once the required number of messages has been received then those messages will be sent to the target actor as an array of messages.

#### Flow Control Actor

The flow control actor is a reliable actor that will send messages to a target actor while ensuring that the number of outstanding (yet to be acknowleged messages) is below the set threshold. This actor is analogous to how TCP flow control works (hence the name).

#### Aggregator

The aggregator will combine messages with the same correlation id and then dispatch as an array to the target. The aggregating actor is created with the types of messages that are required before a "complete" message is ready.

#### Periodic Actor

The periodic actor broadcasts tick messages at a user defined interval. These tick messages can be used by an implementing actor to perform logic based on an interval.

#### Timeout Actor

The timeout actor will terminate itself if a message is not received within a user defined interval. 

#### Keep Alive Actor

The keep alive actor will broadcast a heartbeat at user defined intervals since the last message was received. 

#### Pausable Actor

The pausable actor is a finite state machine will two states - paused or running. If paused then any messages are buffered until the actor is resumed. If the actor is running then all messages are forwarded as normal to the target actor.

#### Splitter

The splitter accepts messages of Iterables and dispatches them singularly.

#### Wiretap

#### Discarding Throttling Actor

The DiscardingThrottlingActor is a rate limiting actor that will send messages with a minimum defined interval. Any messages recevied during this minimum interval will be discarded, with the exception of the most recent, which will be sent once the interval has expired. This pattern is useful for cases such as FX quotes where only the most recent is required.

#### Buffering Throttling Actor

The BufferingThrottlingActor is a rate limiting actor that will send messages with a minimum defined interval. Any messages recevied during this minimum interval will be buffered and replayed at the defined interval. This pattern is useful when an erratic incoming stream is required to be converted to a consistent stream.

#### Dynamic Router

#### Discarding Ackknowledging Actor

The DiscardingAckknowledgingActor is a reliable actor that will send messages as soon as an acknowledgement is received. Any messages recevied during this minimum interval will be discarded with the exception of the most recent and replayed once an ack is received.

#### Buffering Ackknowledging Actor

The BufferingAckknowledgingActor is a reliable actor that will send messages as soon as an acknowledgement is received. Any messages recevied during this minimum interval will be buffered and replayed as acknowledgments arrive.
