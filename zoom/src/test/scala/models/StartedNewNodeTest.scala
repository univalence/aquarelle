package models

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeUnit

import models.OutTopics.GroupEnv
import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, ByteArraySerializer, StringDeserializer, StringSerializer }
import org.scalatest.{ BeforeAndAfterAll, FunSuite }

import scala.util.Try

object BuildInfoTest {

  import zm.BuildInfoZoom._

  val buildInfo: BuildInfo = {

    BuildInfo(
      name = name,
      organization = organization,
      version = version,
      commit = gitHeadCommit.get,
      buildAt = Instant.ofEpochMilli(builtAtMillis)
    )
  }

  val startedNewNode: StartedNewNode =
    StartedNewNode.fromBuild(BuildInfoTest.buildInfo, Environment.Production, UUID.randomUUID())

}

class StartedNewNodeTestV2 extends FunSuite with EmbdedKafkaCustom with EmbeddedKafka with BeforeAndAfterAll {

  import EmbeddedKafkaConfig._

  private val kafkaPort: Int = defaultConfig.kafkaPort

  implicit val keySerializer = new StringSerializer
  implicit val stringDe = new StringDeserializer
  implicit val byteArrDe = new ByteArrayDeserializer

  override def beforeAll(): Unit = {
    EmbeddedKafka.start

  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  test("testFromBuild") {

    import BuildInfoTest._

    val inJson = ZoomEventSerde.toJson(startedNewNode)

    assert(inJson.event_type == "models.StartedNewNode")

    assert(inJson.payload.contains("StartedNewNode"))

    assert(ZoomEventSerde.fromJson[StartedNewNode](inJson.payload).get == startedNewNode)

  }

  test("Embedded Kafka Working") {

    //EmbeddedKafka.publishStringMessageToKafka(topic = "local.zoom.event", message = "")

    Try {
      EmbeddedKafka.consumeFirstMessageFrom[String]("local.zoom.event")
    }
  }

  val testKafkaConfig = KafkaConfiguration(kafkaPort, "localhost")

  ignore("produce low level") {

    val record: ProducerRecord[String, Array[Byte]] = new ProducerRecord[String, Array[Byte]](
      "local.zoom.event",
      null,
      new java.util.Date().getTime,
      null,
      "hello".getBytes,
      null
    )

    def baseProducerConfig: Map[String, Object] = Map[String, Object](
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> testKafkaConfig.kafkaBrokers,
      ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 10000.toString
    ) ++ testKafkaConfig.customProducerProperties

    val producer: KafkaProducer[String, Array[Byte]] = {
      import scala.collection.JavaConverters._
      val config = baseProducerConfig
      new KafkaProducer(config.asJava, new StringSerializer(), new ByteArraySerializer())
    }

    val metadata = producer.send(record).get(10, TimeUnit.SECONDS)
    println(metadata)

  }

  test("Publish To Node") {
    val buildInfo = BuildInfoTest.buildInfo
    val nc = NodeContextV2.createAndStart[Unit](
      group = "datav2",
      environment = Environment.Local,
      kafkaConfiguration = testKafkaConfig,
      buildInfo = buildInfo,
      eventSer = new NCSer[Unit] {
        override def format: EventFormat = EventFormat.Raw
        override def eventType(event: Unit): String = "unit"
        override def serialize(event: Unit): Array[Byte] = Array()
      }
    ).get

    val eventTopic = OutTopics.defaultStrategy(GroupEnv("zoom", Environment.Local)).event

    assert(eventTopic == "local.zoom.event")

    val startedNewNode = ZoomEventSerde.fromJson[StartedNewNode](
      new String(consumeFirstMessageFrom[Array[Byte]](eventTopic))
    ).get

    assert(startedNewNode.environment == Environment.Local)
    assert(startedNewNode.node_id == nc.nodeId)

    nc.stop()

    val sn: StoppedNode = ZoomEventSerde.
      fromJson[StoppedNode](
        new String(
          consumeFirstMessageFrom[Array[Byte]](
            eventTopic
          )
        )
      ).get

    assert(sn.node_id == nc.nodeId)
    //assert(sn.stop_inst > startedNewNode.startup_inst)

  }

}

class StartedNewNodeTest extends FunSuite with EmbdedKafkaCustom with EmbeddedKafka with BeforeAndAfterAll {

  import EmbeddedKafkaConfig._

  val testKafkaConfiguration = KafkaConfiguration(defaultConfig.kafkaPort, "localhost")

  implicit val keySerializer = new StringSerializer
  implicit val stringDe = new StringDeserializer
  implicit val byteArrDe = new ByteArrayDeserializer

  override def beforeAll(): Unit = {
    EmbeddedKafka.stop()
    EmbeddedKafka.start
  }

  override def afterAll(): Unit = {
    EmbeddedKafka.stop()
  }

  test("testFromBuild") {

    import BuildInfoTest._

    val inJson = ZoomEventSerde.toJson(startedNewNode)

    assert(inJson.event_type == "models.StartedNewNode")

    assert(inJson.payload.contains("StartedNewNode"))

    assert(ZoomEventSerde.fromJson[StartedNewNode](inJson.payload).get == startedNewNode)

  }

  ignore("Publish To Node") {

    ???

    implicit val buildInfo = BuildInfoTest.buildInfo
    new NodeContext(environment = Environment.Local, kafkaConfiguration = testKafkaConfiguration)

    val fm = new String(consumeFirstMessageFrom[Array[Byte]]("data.event"))
    /*val fromJson = EventSerde.fromJson[StartedNewNode](fm)

    assert(fromJson.isSuccess)*/

    /*val event = fromJson.get
    assert(event.prg_name == buildInfo.name)
    assert(event.environment == Environment.Recette)
    assert(event.node_hostname.nonEmpty)*/

  }

}

