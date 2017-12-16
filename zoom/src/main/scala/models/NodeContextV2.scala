package models

import java.util.UUID


case class OutTopics(log: String, event: String, raw: String)

object OutTopics {

  case class GroupEnv(group: String, environment: Environment)
  type Strategy = GroupEnv => OutTopics

  @deprecated(message = "please use default strategy")
  val oldStrategy: Strategy = {
    case GroupEnv(_, env) =>
      val shortname = env.shortname
      OutTopics(
        log = "logs." + shortname,
        event = "data.event." + shortname,
        raw = "data.raw." + shortname)
  }


  val defaultStrategy: Strategy = {
    case GroupEnv(group, env) =>
      val shortname = env.shortname
      OutTopics(
        log = s"$shortname.$group.log",
        event = s"$shortname.$group.event",
        raw = s"$shortname.$group.raw")
  }

}

class NodeContextV2(val group: String,
                    val environment: Environment,
                    val kafkaConfiguration: KafkaConfiguration = KafkaConfiguration.defaultKafkaConfiguration,
                    val buildInfo: BuildInfo,
                    val topicStrategy: OutTopics.Strategy = OutTopics.defaultStrategy
                   ) extends Serializable {

  ??? // do not use yet
  //TODO : Find how other let user use their event definition
  //Take a look at akka persistence or eventuate or Lagom
  //add a type T for event ? NodeContext[T]

  @deprecated(message = "pass buildinfo directly or use macwire")
  @deprecated(message = "should specify group")
  def this(environment: Environment)(implicit buildInfo: BuildInfo) = {
    this(environment = environment, buildInfo = buildInfo, topicStrategy = OutTopics.oldStrategy)
  }


  val nodeId: UUID = UUID.randomUUID()
  val nodeTracingContext: Tracing = Tracing()


}
