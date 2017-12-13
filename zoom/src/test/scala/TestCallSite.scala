package toto

import models.macros.Callsite

object TestCallSite {

  def main(args: Array[String]): Unit = {

    println(Callsite.callSite)

  }
}