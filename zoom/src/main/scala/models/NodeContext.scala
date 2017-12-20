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
  kafkaHost:                String              = "trafgar01t.bbo1t.local",
  customBrokerProperties:   Map[String, String] = Map.empty,
  customProducerProperties: Map[String, String] = Map.empty
) {

  def kafkaBrokers = s"$kafkaHost:$kafkaPort"
}

object KafkaConfiguration {
  def defaultKafkaConfiguration: KafkaConfiguration = new KafkaConfiguration(9092)
}

trait LoggerWithCtx[Context] {
  def info(message: ⇒ String)(implicit context: Context): Unit
  def warn(message: ⇒ String)(implicit context: Context): Unit
  def fatal(message: ⇒ String)(implicit context: Context): Unit
  def error(message: ⇒ String)(implicit context: Context): Unit
  def debug(message: ⇒ String)(implicit context: Context): Unit
}

private trait LoggerImplWithCtx[Context] extends LoggerWithCtx[Context] {
  def log(message: ⇒ String, level: String)(implicit context: Context): Unit

  final override def info(message: ⇒ String)(implicit context: Context): Unit = log(message, "info")
  final override def warn(message: ⇒ String)(implicit context: Context): Unit = log(message, "warn")
  final override def fatal(message: ⇒ String)(implicit context: Context): Unit = log(message, "fatal")
  final override def error(message: ⇒ String)(implicit context: Context): Unit = log(message, "error")
  final override def debug(message: ⇒ String)(implicit context: Context): Unit = log(message, "debug")
}

case class TracingAndCallSite(implicit val tracing: Tracing, implicit val callsite: Callsite)

object TracingAndCallSite {
  implicit def fromTracingAndCallSite(implicit tracing: Tracing, callsite: Callsite): TracingAndCallSite = TracingAndCallSite()
}

trait Logger extends LoggerWithCtx[TracingAndCallSite]

trait NodeLogger extends LoggerWithCtx[Callsite]

case class NodeInfo(node_id: UUID)

class NodeContext(
  val environment:    Environment,
  private val nodeId: UUID               = UUID.randomUUID(),
  kafkaConfiguration: KafkaConfiguration = KafkaConfiguration.defaultKafkaConfiguration,
  nodeTracingContext: Tracing            = Tracing()
)(implicit buildInfo: BuildInfo) extends Serializable {

  //tochangea
  val kafkaBrokers: String = kafkaConfiguration.kafkaHost + ":" + kafkaConfiguration.kafkaPort

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
    saveEvent(StartedNewNode.fromBuild(buildInfo, environment, nodeId))(nodeTracingContext, Callsite.callSite)
  }

  start()

  def saveEvent[E <: ZoomEvent](
    event:    E,
    event_id: UUID = UUID.randomUUID()
  )(implicit tracingContext: Tracing, callsite: Callsite): Future[Unit] = {

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
  )(implicit tracingContext: Tracing, callsite: Callsite): Future[Unit] = {

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

  def getLogger(logClass: Class[_]): Logger = new LoggerImplWithCtx[TracingAndCallSite] with Logger {
    override def log(message: ⇒ String, level: String)(implicit context: TracingAndCallSite): Unit = {
      import context._
      println(s"**$level : $message")
      publishRaw(
        content = message.getBytes,
        topic = "logs." + environment.shortname,
        event_format = EventFormat.Raw,
        event_type = s"logs/$level/" + logClass.getName,
        key = Some(nodeId.toString)
      )
    }
  }

  def getNodeLogger: NodeLogger = new NodeLogger with LoggerImplWithCtx[Callsite] {
    override def log(message: ⇒ String, level: String)(implicit context: Callsite): Unit = {
      implicit val t = nodeTracingContext
      publishRaw(
        content = message.getBytes,
        topic = "logs." + environment.shortname,
        event_format = EventFormat.Raw,
        event_type = s"logs/$level/" + this.getClass.getName,
        key = Some(nodeId.toString)
      )
    }
  }

}