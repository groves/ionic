package com.bungleton.ionic.store

import com.threerings.fisy.Directory
import org.apache.avro.Schema
import org.apache.avro.io.Decoder
import com.threerings.fisy.Record

class BooleanAvroColumn (val decoder :Decoder, val dest :Directory, val field :String) {
  private val out = dest.open(field).write()
  def write() {
    out.write(if (decoder.readBoolean()) 1 else 0)
  }
}
