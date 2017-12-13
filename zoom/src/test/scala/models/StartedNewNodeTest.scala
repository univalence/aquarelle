package models

import java.time.Instant
import java.util.UUID

import net.manub.embeddedkafka.{ EmbeddedKafka, EmbeddedKafkaConfig }
import org.apache.kafka.common.serialization.{ ByteArrayDeserializer, StringDeserializer, StringSerializer }
import org.scalatest.{ BeforeAndAfterAll, FunSuite }

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

class StartedNewNodeTest extends FunSuite with EmbdedKafkaCustom with EmbeddedKafka with BeforeAndAfterAll {

  import EmbeddedKafkaConfig._
  val testKafkaConfiguration = KafkaConfiguration(defaultConfig.kafkaPort)

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

    val inJson = EventSerde.toJson(startedNewNode)

    assert(inJson.event_type == "models.StartedNewNode")

    assert(inJson.payload.contains("StartedNewNode"))

    assert(EventSerde.fromJson[StartedNewNode](inJson.payload).get == startedNewNode)

  }

  ignore("Publish To Node") {
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
