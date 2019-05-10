package com.sksamuel.reactivehive.akka.streams

import akka.stream.Attributes
import akka.stream.Inlet
import akka.stream.SinkShape
import akka.stream.stage.GraphStage
import akka.stream.stage.GraphStageLogic
import akka.stream.stage.InHandler
import com.sksamuel.reactivehive.Struct

class HiveSink : GraphStage<SinkShape<Struct>>() {

  private val inlet: Inlet<Struct> = Inlet.create("HiveSink.in")

  override fun shape(): SinkShape<Struct> = SinkShape.of(inlet)

  override fun createLogic(attrs: Attributes?): GraphStageLogic = object : GraphStageLogic(shape()), InHandler {
    override fun onPush() {
      val struct = grab(inlet)

    }
  }
}