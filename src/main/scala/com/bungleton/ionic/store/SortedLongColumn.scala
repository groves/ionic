package com.bungleton.ionic.store

import org.apache.avro.Schema
import com.threerings.fisy.Directory
import org.apache.avro.io.Decoder
import org.apache.avro.io.EncoderFactory

class SortedLongColumn (decoder :Decoder, dest :Directory, field :Schema.Field) extends Column {
  private val out = dest.open(field.name).write()
  private val encoder = EncoderFactory.get().directBinaryEncoder(out, null)

  private var previous :Long = 0
  private var offset :Long = 0
  private var countAtPrevious :Long = 0

  def write() {
    val current = decoder.readLong()

    if (current != previous) {
      if (countAtPrevious > 0) {
        encoder.writeLong(offset)
        encoder.writeLong(countAtPrevious)
      }
      offset = current - previous
      previous = current
      countAtPrevious = 1
    } else {
      countAtPrevious += 1
    }
  }


  def close () {
    if (countAtPrevious > 0) {
      encoder.writeLong(offset)
      encoder.writeLong(countAtPrevious)
    }
    out.close()
  }
}
