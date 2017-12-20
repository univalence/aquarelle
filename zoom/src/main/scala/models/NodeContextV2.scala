package models

import java.nio.charset.Charset
import java.util.UUID

import models.macros.Callsite
import org.apache.kafka.clients.producer.{ KafkaProducer, ProducerConfig }
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
      EventSerde.toJson(event).
        payload.
        getBytes(Charset.forName("UTF_8"))

    override def format: EventFormat = EventFormat.CCJson

    override def eventType(event: ZoomEvent): String = event.getClass.getName
  }

}

object NoteContext {

  //TODO : Use a smart constructor to make sure that cannot be get an instance without a valid connexion to Kafka

}

class NodeContextV2[Event] private (
  val group:              String,
  val environment:        Environment,
  val kafkaConfiguration: KafkaConfiguration = KafkaConfiguration.defaultKafkaConfiguration,
  val buildInfo:          BuildInfo,
  val eventSer:           NCSer[Event],
  val topicStrategy:      OutTopics.Strategy = OutTopics.defaultStrategy,
  val zoomGroupName:      String             = "zoom"
) extends Serializable {

  ??? // do not use yet
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

  private def baseProducerConfig: Map[String, Object] = Map[String, Object](
    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConfiguration.kafkaBrokers,
    ProducerConfig.MAX_BLOCK_MS_CONFIG -> 10000.toString,
    ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> 1000.toString
  ) ++ kafkaConfiguration.customProducerProperties

  val nodeId: UUID = UUID.randomUUID()
  val nodeTracingContext: Tracing = Tracing()

  lazy val producer: KafkaProducer[String, Array[Byte]] = {
    import scala.collection.JavaConverters._
    new KafkaProducer(baseProducerConfig.asJava, new StringSerializer(), new ByteArraySerializer())
  }

  private def start(): Unit = {
    //publishStartedNode into zoom.event.event ?
    //saveEvent(StartedNewNode.fromBuild(buildInfo, environment, nodeId))(nodeTracingContext, Callsite.callSite)
  }

  def heartbeat(): Unit = {
    //
    //
    //log compaction ??
  }

  def stop(): Unit = {
    //publish
    //
    //
  }

  start()

  def saveEvent(event: Event)(implicit tracingContext: Tracing, callsite: Callsite): Future[Unit] = {
    val event_id: UUID = UUID.randomUUID()

    /*val json = EventSerde.toJson(event)
    publishRaw(json.payload.getBytes, "data.event." + environment.shortname, EventFormat.CCJson,
      event_type = json.event_type, event_id)*/
    ???
  }

  def publishRaw(content: Array[Byte], format: EventFormat)(implicit
    tracing: Tracing,
                                                            callsite: Callsite
  ): Future[Unit] = {
    ???
  }

  /*
  private def publishRaw(
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
  */

  /*
  private def sendToKafka(topic: String, key: Option[String], content: Array[Byte], headers: Seq[(String, Array[Byte])]): Future[Unit] = {

    import scala.collection.JavaConverters._
    import scala.concurrent.ExecutionContext.Implicits.global

    val hdrs = headers.map(t ⇒ new RecordHeader(t._1, t._2).asInstanceOf[Header])
    val record: ProducerRecord[String, Array[Byte]] = new ProducerRecord[String, Array[Byte]](topic, null, new java.util.Date().getTime, key.orNull, content, hdrs.asJava)
    Future {
      producer.send(record).get
    }

  }
   */

  def getLogger(logClass: Class[_]): Logger = new LoggerImplWithCtx[TracingAndCallSite] with Logger {
    override def log(message: ⇒ String, level: String)(implicit context: TracingAndCallSite): Unit = {
      /*println(s"**$level : $message")
      publishRaw(
        content = message.getBytes,
        topic = "logs." + environment.shortname,
        event_format = EventFormat.Raw,
        event_type = s"logs/$level/" + logClass.getName,
        key = Some(nodeId.toString)
      )*/
    }
  }

  lazy val rootLog: NodeLogger = new NodeLogger with LoggerImplWithCtx[Callsite] {
    override def log(message: ⇒ String, level: String)(implicit context: Callsite): Unit = {
      implicit val t = nodeTracingContext
      /*publishRaw(
        content = message.getBytes,
        topic = "logs." + environment.shortname,
        event_format = EventFormat.Raw,
        event_type = s"logs/$level/" + this.getClass.getName,
        key = Some(nodeId.toString)
      )*/
    }
  }
}

