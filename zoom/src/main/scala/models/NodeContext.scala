package models

import java.util.UUID

import models.macros.Callsite
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }
//import utils.Configuration

import scala.concurrent.Future

//trait KafkaConfiguration

case class KafkaConfiguration(
  kafkaPort:                Int,
  customBrokerProperties:   Map[String, String] = Map.empty,
  customProducerProperties: Map[String, String] = Map.empty
)

object KafkaConfiguration {

  def defaultKafkaConfiguration: KafkaConfiguration = new KafkaConfiguration(9092)

}

trait Logger {
  def info(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit

  def warn(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit

  def fatal(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit

  def error(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit

  def debug(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit
}

case class NodeInfo(node_id: UUID)

class NodeContext(
  val environment:    Environment,
  private val nodeId: UUID               = UUID.randomUUID(),
  kafkaConfiguration: KafkaConfiguration = KafkaConfiguration.defaultKafkaConfiguration
)(implicit buildInfo: BuildInfo) extends Serializable {

  //tochange
  val kafkaBrokers = s"trafgar01t.bbo1t.local:9092"

  def baseProducerConfig: Map[String, Object] = Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrokers,
    ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
    ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
  ) ++ kafkaConfiguration.customProducerProperties

  lazy val producer: KafkaProducer[String, Array[Byte]] = {

    import scala.collection.JavaConverters._
    new KafkaProducer(baseProducerConfig.asJava, new StringSerializer(), new ByteArraySerializer())
  }

  private def start(): Unit = {
    saveEvent(StartedNewNode.fromBuild(buildInfo, environment, nodeId))(Tracing(), Callsite.callSite)
  }

  start()

  def saveEvent[E <: Event](
    event:    E,
    event_id: UUID = UUID.randomUUID()
  )(implicit tracingContext: TracingContext, callsite: Callsite): Future[Unit] = {

    val json = EventSerde.toJson(event)
    publishRaw(json.payload.getBytes, "data.event." + environment.shortname, EventFormat.CCJson, event_type = json.event_type, event_id)
  }

  def publishRaw(
    content:      Array[Byte],
    topic:        String         = "data.raw." + environment.shortname,
    event_format: EventFormat,
    event_type:   String,
    event_id:     UUID           = UUID.randomUUID(),
    key:          Option[String] = None
  )(implicit tracingContext: TracingContext, callsite: Callsite): Future[Unit] = {

    val meta = EventMetadata(
      event_id = event_id,
      event_type = event_type,
      event_format = event_format,
      trace_id = tracingContext.getTraceId,
      parent_span_id = tracingContext.getParentSpanId,
      previous_span_id = tracingContext.getPreviousSpanId,
      span_id = tracingContext.getSpanId,
      node_id = nodeId,
      env = environment, callsite = Some(callsite),
      on_behalf_of = tracingContext.getOnBehalfOf
    )

    sendToKafka(topic, key, content, meta.toStringMap.mapValues(_.getBytes).toSeq)
  }

  private def sendToKafka(topic: String, key: Option[String], content: Array[Byte], headers: Seq[(String, Array[Byte])]): Future[Unit] = {

    import scala.collection.JavaConverters._
    import scala.concurrent.ExecutionContext.Implicits.global

    val hdrs = headers.map(t ⇒ new RecordHeader(t._1, t._2).asInstanceOf[Header])
    val record: ProducerRecord[String, Array[Byte]] = new ProducerRecord[String, Array[Byte]](topic, null, new java.util.Date().getTime, key.orNull, content, hdrs.asJava)
    Future {
      producer.send(record).get
    }

  }

  def getLogger(context: Class[_]): Logger = new Logger {
    private def log(message: ⇒ String, level: String)(implicit tracingContext: TracingContext, callsite: Callsite): Unit = {
      println(s"**$level : $message")
      publishRaw(
        content = message.getBytes,
        topic = "logs." + environment.shortname,
        event_format = EventFormat.Raw,
        event_type = s"logs/$level/" + context.getName,
        key = Some(nodeId.toString)
      )
    }

    override def error(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit = log(message, "error")

    override def fatal(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit = log(message, "fatal")

    override def warn(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit = log(message, "warn")

    override def info(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit = log(message, "info")

    override def debug(message: ⇒ String)(implicit tracingContext: TracingContext, callSite: Callsite): Unit = log(message, "debug")
  }

}