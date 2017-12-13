import java.time.Instant

import models._
import zm.BuildInfoZoom._

object sendNC extends {

  implicit val buildInfo: BuildInfo = {

    BuildInfo(
      name = name,
      organization = organization,
      version = version,
      commit = gitHeadCommit.get,
      buildAt = Instant.ofEpochMilli(builtAtMillis)
    )
  }
  implicit val tc = Tracing()
  val nc = new NodeContext(Environment.Recette)

  def main(args: Array[String]): Unit = {

    while (true) {
      nc.publishRaw("yo".getBytes, event_format = EventFormat.Raw, event_type = "test")
      Thread.sleep(100000)
    }
  }
}
