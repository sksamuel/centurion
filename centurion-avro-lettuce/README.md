# Centurion Avro Lettuce

This module provides integration between [Centurion Avro](../centurion-avro) and [Lettuce](https://lettuce.io/), a Redis client for Java.

## Usage

### Creating a client

To create an `AvroLettuceClient`, you need a Redis client and a Serde:

```kotlin
// Create a Redis client
val redisClient = RedisClient.create("redis://localhost:6379")

// Create a BinarySerde for your data class
val serde = BinarySerde(
    YourDataClass::class,
    EncoderFactory.get(),
    DecoderFactory.get()
)

// Create an AvroLettuceClient
val client = AvroLettuceClientFactory.create(redisClient, serde)
```

Or, if you prefer to create the client directly:

```kotlin
val client = AvroLettuceClient(redisClient, serde)
```

### Storing and retrieving data

```kotlin
// Store data
val data = YourDataClass(/* ... */)
client.set("key", data)

// Retrieve data
val retrievedData = client.get("key")
```

### Closing the client

```kotlin
client.close()
```

## Dependencies

This module depends on:

- `centurion-avro`: For Avro serialization and deserialization
- `lettuce-core`: For Redis client functionality
