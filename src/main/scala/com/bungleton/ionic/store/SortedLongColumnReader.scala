package com.bungleton.ionic.store

import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.DecoderFactory
import com.threerings.fisy.Directory
import org.apache.avro.Schema

class SortedLongColumnReader(source: Directory, field: Schema.Field)
  extends ColumnReader {
  private val in = source.open(field.name).read()
  private val decoder = DecoderFactory.get().binaryDecoder(in, null)

  private var value: Long = 0
  private var countAtValue: Long = 0
  private var closed: Boolean = false

  def read(rec: IndexedRecord) {
    if (countAtValue == 0) {
      value += decoder.readLong()
      countAtValue = decoder.readLong()
    }
    countAtValue -= 1
    rec.put(field.pos, value)
  }

  def close() { in.close() }
}
