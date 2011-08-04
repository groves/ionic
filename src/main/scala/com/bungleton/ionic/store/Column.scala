package com.bungleton.ionic.store

import org.apache.avro.io.Decoder

trait Column {
  def write (decoder :Decoder)
  def close ()
}
