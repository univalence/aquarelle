/*package utils

import com.typesafe.config.{ Config, ConfigException, ConfigFactory }
import models.Environment
import models.Environment.Production

import scala.util.Try

case class TGSettings(webservice: WSSettings, graphqlservice: GQLSettings, common: CommonSettings)

case class WSSettings(port: Int, hostname: String, deploypath: String)

case class GQLSettings(port: Int, hostname: String, vaadinPort: Int, deploypath: String) {
  def fullurl = "http://" + hostname + ":" + port
}

case class CommonSettings(urlGpp: String, kafkaHost: String) {
  def urlGppEnrichissement = urlGpp + "/cxf/soap/gestiongpp/enrichissementgpp"
  def urlGppRecuperation = urlGpp + "/cxf/soap/gestiongpp/donneesgpp"
}

object Configuration {

  lazy val config: Map[Environment, TGSettings] = Environment.all.flatMap { env ⇒
    Try {
      val config = ConfigFactory.load("trafgar").getConfig(env.shortname)
      env -> TGSettings(
        webservice = WSSettings(
          config.getInt("tg-webservice.serverWebPort"),
          config.getString("tg-webservice.hostname"),
          config.getString("tg-webservice.deploypath")
        ),
        graphqlservice = GQLSettings(
          config.getInt("tg-graphql.serverWebPort"),
          config.getString("tg-graphql.hostname"),
          config.getInt("tg-graphql.vaadinPort"),
          config.getString("tg-graphql.deploypath")
        ),
        common = CommonSettings(
          config.getString("common.urlGpp"),
          config.getString("common.kafkaHost")
        )
      )
    }.toOption
  }.toMap

  def main(args: Array[String]): Unit = {

    config.foreach(println)

    println(config(Environment.Local).graphqlservice.fullurl)

  }

}*/ 