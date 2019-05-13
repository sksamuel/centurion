package com.sksamuel.rxhive

import org.slf4j.LoggerFactory

interface Logging {
  val logger
    get() = LoggerFactory.getLogger(this.javaClass)
}
