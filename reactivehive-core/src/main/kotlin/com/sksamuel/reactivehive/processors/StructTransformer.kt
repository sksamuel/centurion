package com.sksamuel.reactivehive.processors

import com.sksamuel.reactivehive.Struct

/**
 * Implementations transform structs before they are written (for sinks) or after they
 * are read (for sources).
 */
interface StructTransformer {
  fun process(struct: Struct): Struct
}

