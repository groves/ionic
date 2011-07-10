package com.bungleton.ionic.store

import com.threerings.fisy.Directory
import org.apache.avro.io.Decoder

class BooleanAvroColumn (decoder :Decoder, dest :Directory, field :String) {
  private val out = dest.open(field).write()
  def write() {
    out.write(if (decoder.readBoolean()) 1 else 0)
  }
}
