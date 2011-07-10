package com.bungleton.ionic.store

import com.google.common.io.CountingInputStream
import java.io.InputStream
import org.testng.Reporter
import com.threerings.fisy.Directory
import org.apache.avro.io.Encoder
import org.apache.avro.io.Decoder
import org.testng.Assert._
import com.threerings.fisy.Paths
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema
import org.testng.annotations.Test

import scala.collection.JavaConversions._
import org.apache.avro.Schema.Type._

class WriteSimpleColumns {
  def createRecord (name :String, fields :List[Schema.Field]) = {
    val record = Schema.createRecord(name, "", "", false)
    record.setFields(fields)
    record
  }

  def createField (name :String, stype :Schema.Type) = {
    new Schema.Field(name, Schema.create(stype), "", null)
  }

  def check[T] (defs :List[Tuple3[String, Schema.Type, List[T]]], enc :((Encoder, T) => Unit),
    dec :((Decoder, List[T]) => Unit)) {

    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    0 until defs(0)._3.length foreach(x => defs.foreach(f => enc(encoder, f._3(x))))

    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    val root = Paths.makeMemoryFs()
    val rec = createRecord("testrec", defs.map(f => createField(f._1, f._2)))
    val writer = new Writer(rec, decoder, root)
    0 until defs(0)._3.length foreach (_ => writer.write)
    writer.close()

    defs.foreach { case (name, _, values) => {
      val in = root.open(name).read()
      val decoder = DecoderFactory.get().binaryDecoder(in, null)
      dec(decoder, values)
      assertEquals(in.read(), -1)
    }}
  }

  @Test def writeBooleans () = {
    check[Boolean](List(("bool1", BOOLEAN, List(true)), ("bool2", BOOLEAN, List(false))),
      (enc, value) => enc.writeBoolean(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readBoolean(), x)))
  }

  @Test def writeInts () = {
    check[Int](List(("int1", INT, List(0, 0, 0)),
      ("int2", INT, List(1, 2, 3)),
      ("int3", INT, List(101010, 10101010, 1010101010))),
      (enc, value) => enc.writeInt(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readInt(), x)))
  }

  @Test def writeSortedLong () = {
    check[Long](List(("timestamp", LONG,
      List(1000, 1000, 1000, 1000, 1001, 1001, 1001, 1002, 2002, 2002))),
      (enc, value) => enc.writeLong(value),
      (dec, values) => {
        assertEquals(dec.readLong(), 1000)
        assertEquals(dec.readLong(), 4)
        assertEquals(dec.readLong(), 1)
        assertEquals(dec.readLong(), 3)
        assertEquals(dec.readLong(), 1)
        assertEquals(dec.readLong(), 1)
        assertEquals(dec.readLong(), 1000)
        assertEquals(dec.readLong(), 2)
       })
  }
}
