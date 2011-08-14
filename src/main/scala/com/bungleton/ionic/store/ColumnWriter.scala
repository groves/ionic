package com.bungleton.ionic.store

import org.apache.avro.io.Decoder

trait ColumnWriter {
  def write(decoder: Decoder)
  def close()
}
