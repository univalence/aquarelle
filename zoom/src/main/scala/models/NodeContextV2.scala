package models

import java.nio.charset.Charset
import java.time.Instant
import java.util.UUID

import models.OutTopics.GroupEnv
import models.macros.Callsite
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig, ProducerRecord }
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.{ ByteArraySerializer, StringSerializer }

import scala.concurrent.Future

case class OutTopics(log: String, event: String, raw: String)

object OutTopics {

  case class GroupEnv(group: String, environment: Environment)

  type Strategy = GroupEnv ⇒ OutTopics

  @deprecated(message = "please use default strategy")
  val oldStrategy: Strategy = {
    case GroupEnv(_, env) ⇒
      val shortname = env.shortname
      OutTopics(
        log = "logs." + shortname,
        event = "data.event." + shortname,
        raw = "data.raw." + shortname
      )
  }

  val defaultStrategy: Strategy = {
    case GroupEnv(group, env) ⇒
      val shortname = env.shortname
      OutTopics(
        log = s"$shortname.$group.log",
        event = s"$shortname.$group.event",
        raw = s"$shortname.$group.raw"
      )
  }
}

trait NCSer[Event] {
  def serialize(event: Event): Array[Byte]

  def format: EventFormat

  def eventType(event: Event): String
}

object NCSer {
  @deprecated(message = "to move in trafic garanti")
  val tgEventSerde: NCSer[ZoomEvent] = new NCSer[ZoomEvent] {
    override def serialize(event: ZoomEvent): Array[Byte] =
      ZoomEventSerde.toJson(event).
        payload.
        getBytes(Charset.forName("UTF_8"))

    override def format: EventFormat = EventFormat.CCJson

    override def eventType(event: ZoomEvent): String = event.getClass.getName
  }

}

object NoteContextV2 {

  //TODO : Use a smart constructor to make sure that cannot be get an instance without a valid connexion to Kafka

  //??? // do not use yet
  //TODO : Find how other let user use their event definition
  //Take a look at akka persistence or eventuate or Lagom
  //add a type T for event ? NodeContext[T]
  /*
    @deprecated(message = "pass buildinfo directly or use macwire")
    @deprecated(message = "should specify group")
    @deprecated(message = "use the smart factory")
    def this(environment: Environment)(implicit buildInfo: BuildInfo) = {
      this(group = "data",
        environment = environment,
        buildInfo = buildInfo,
        topicStrategy = OutTopics.oldStrategy,
        eventSer = NCSer.tgEventSerde)
    }
    */

}

class NodeContextV2[Event] protected (
  val group:              String,
  val environment:        Environment,
  val kafkaConfiguration: KafkaConfiguration = KafkaConfiguration.defaultKafkaConfiguration,
  val buildInfo:          BuildInfo,
  val eventSer:           NCSer[Event],
  val topicStrategy:      OutTopics.Strategy = OutTopics.defaultStrategy,
  val zoomGroupName:      String             = "zoom"
) extends Serializable {

  private object nodeElement {
    val nodeId: UUID = UUID.randomUUID()
    private val nodeTracingContext: Tracing = Tracing()
    private val nodeOutTopics: OutTopics = topicStrategy(GroupEnv("zoom", environment))

    def start(): Unit = {
      val json = ZoomEventSerde.toJson(StartedNewNode.fromBuild(
        buildInfo = buildInfo,
        environment = environment,
        node_id = nodeId
      ))

      publishLow(
        topic = nodeOutTopics.event,
        content = json.payload.getBytes(UTF8_CHARSET),
        format = EventFormat.CCJson,
        eventType = json.event_type,
        tracing = nodeTracingContext,
        callsite = implicitly[Callsite]
      )
    }

    def stop(): Unit = {
      val json = ZoomEventSerde.toJson(StoppedNode(
        node_id = nodeId,
        stop_inst = Instant.now,
        cause = "shutdown hook",
        more = Map.empty
      ))

      publishLow(
        topic = nodeOutTopics.event,
        content = json.payload.getBytes(UTF8_CHARSET),
        format = EventFormat.CCJson,
        eventType = json.event_type,
        tracing = nodeTracingContext,
        callsite = implicitly[Callsite]
      )

    }

    val logger = new NodeLogger with LoggerImplWithCtx[Callsite] {
      override def log(message: ⇒ String, level: String)(implicit context: Callsite): Unit = {
        implicit val t = nodeTracingContext

        publishLow(
          topic = nodeOutTopics.log,
          content = message.getBytes(UTF8_CHARSET),
          format = EventFormat.Raw,
          eventType = s"logs/$level" + this.getClass.getName,
          tracing = nodeTracingContext,
          callsite = implicitly[Callsite]
        )
      }
    }
  }

  private val groupOutTopics: OutTopics = topicStrategy(GroupEnv(group, environment))

  private val UTF8_CHARSET: Charset = java.nio.charset.Charset.forName("UTF-8")

  private def baseProducerConfig: Map[String, Object] = Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConfiguration.kafkaBrokers,
    ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
    ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
  ) ++ kafkaConfiguration.customProducerProperties

  private val producer: KafkaProducer[String, Array[Byte]] = {
    import scala.collection.JavaConverters._
    new KafkaProducer(baseProducerConfig.asJava, new StringSerializer(), new ByteArraySerializer())
  }

  nodeElement.start()

  {
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        nodeElement.stop()
      }
    })
  }

  def publishEvent(event: Event)(implicit tracing: Tracing, callsite: Callsite): Future[Unit] = {

    val content = eventSer.serialize(event)
    val eventType = eventSer.eventType(event)

    publishLow(groupOutTopics.event, content, EventFormat.CCJson, eventType, tracing, callsite)
  }

  private def publishLow(topic: String, content: Array[Byte], format: EventFormat, eventType: String, tracing: Tracing, callsite: Callsite): Future[Unit] = {

    val meta = EventMetadata(
      event_id = UUID.randomUUID(),
      event_type = eventType,
      event_format = format,
      trace_id = tracing.getTraceId,
      parent_span_id = tracing.getParentSpanId,
      previous_span_id = tracing.getPreviousSpanId,
      span_id = tracing.getSpanId,
      node_id = nodeElement.nodeId,
      env = environment, callsite = Some(callsite),
      on_behalf_of = tracing.getOnBehalfOf
    )

    import scala.collection.JavaConverters._
    import scala.concurrent.ExecutionContext.Implicits.global
    val headers = meta.toStringMap.mapValues(_.getBytes).toSeq

    val hdrs = headers.map(t ⇒ new RecordHeader(t._1, t._2).asInstanceOf[Header])
    val record: ProducerRecord[String, Array[Byte]] = new ProducerRecord[String, Array[Byte]](
      topic,
      null,
      new java.util.Date().getTime,
      null,
      content,
      hdrs.asJava
    )

    Future {
      producer.send(record).get
    }
  }

  def publishRaw(
    content:   Array[Byte],
    format:    EventFormat,
    eventType: String
  )(
    implicit
    tracing:  Tracing,
    callsite: Callsite
  ): Future[Unit] = {
    publishLow(groupOutTopics.raw, content, EventFormat.Raw, eventType, tracing, callsite)
  }

  def getLogger(logClass: Class[_]): Logger = new LoggerImplWithCtx[TracingAndCallSite] with Logger {
    override def log(
      message: ⇒ String,
      level:   String
    )(implicit context: TracingAndCallSite): Unit = {

      publishLow(
        topic = groupOutTopics.log,
        content = message.getBytes(UTF8_CHARSET),
        format = EventFormat.Raw,
        eventType = s"logs/$level/" + logClass.getName,
        tracing = context.tracing,
        callsite = context.callsite
      )
    }
  }

  def rootLog: NodeLogger = nodeElement.logger
}

