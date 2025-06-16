package com.sksamuel.centurion.avro.lettuce

import com.redis.testcontainers.RedisContainer
import io.kotest.extensions.testcontainers.ContainerExtension
import io.lettuce.core.ReadFrom
import io.lettuce.core.RedisURI
import io.lettuce.core.cluster.ClusterClientOptions
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions
import io.lettuce.core.cluster.RedisClusterClient
import io.lettuce.core.cluster.SlotHash
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.resource.ClientResources
import io.lettuce.core.resource.SocketAddressResolver
import org.testcontainers.containers.GenericContainer
import java.net.InetSocketAddress
import java.net.SocketAddress
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toJavaDuration

private const val DOCKER_IMAGE_NAME = "grokzen/redis-cluster:7.0.10"

private val redisClusterContainer = RedisContainer(DOCKER_IMAGE_NAME)
   .withExposedPorts(7000, 7001, 7002, 7003, 7004, 7005)

val resolver = object : SocketAddressResolver() {
   override fun resolve(redisURI: RedisURI): SocketAddress {
      return if (redisURI.port > 7005) {
         InetSocketAddress.createUnresolved("localhost", redisURI.port)
      } else {
         redisURI.port = redisClusterContainer.getMappedPort(redisURI.port)
         InetSocketAddress.createUnresolved("localhost", redisURI.port)
      }
   }
}

val redisExtension: ContainerExtension<RedisContainer> = ContainerExtension(redisClusterContainer)
private const val DEFAULT_STARTUP_ATTEMPTS = 200

fun <K, V> GenericContainer<*>.toConnection(
   readFrom: ReadFrom = ReadFrom.MASTER,
   codec: RedisCodec<K, V>,
   attempts: Int = DEFAULT_STARTUP_ATTEMPTS
): StatefulRedisClusterConnection<K, V> {

   val uri = RedisURI.Builder.redis("localhost", 7000).build()

   val resources = ClientResources.builder()
      .socketAddressResolver(resolver)
      .build()

   val topologyRefreshOptions = ClusterTopologyRefreshOptions.builder()
      .enableAllAdaptiveRefreshTriggers()
      .adaptiveRefreshTriggersTimeout(1.seconds.toJavaDuration())
      .build()

   val clusterClient: RedisClusterClient = RedisClusterClient.create(resources, uri).apply {
      setOptions(
         ClusterClientOptions
            .builder()
            .topologyRefreshOptions(topologyRefreshOptions)
            .build()
      )
   }

   fun waitForCluster(attempts: Int) {
      try {
         val connection = clusterClient.connect().apply {
            this.readFrom = readFrom
         }
         val clusters = connection.sync().clusterNodes().trim().lines()
         require(clusters.size == 6) // 3 masters + 3 slaves
         val nodes = connection.sync().clusterSlots()
         val slots = nodes.sumOf {
            val bits = it as List<Any>
            bits[1].toString().toInt() - bits[0].toString().toInt() + 1
         }
         require(slots == SlotHash.SLOT_COUNT)
         // fake gets to check the cluster is up
         connection.sync().get("a")
         connection.sync().get("b")
         connection.sync().get("c")
      } catch (_: Exception) {
         if (attempts == 0) error("Could not connect to redis container after $attempts attempts")
         Thread.sleep(100)
         waitForCluster(attempts - 1)
      }
   }

   waitForCluster(attempts)
   return clusterClient.connect(codec)
}
