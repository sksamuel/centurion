akka-patterns
=============

Implementations of EIPs using Akka


#### Release

The latest release is 0.10.0

[![Build Status](https://travis-ci.org/sksamuel/akka-patterns.png)](https://travis-ci.org/sksamuel/akka-patterns)

### Patterns

#### Enveloping Actor

The enveloping actor will wrap any incoming messages in an Envelope object, together with attributes describing that messsage. This pattern is used when you need to associate metadata with the message. The typical example would be when a correlation id is needed for a message exchange.

#### 

