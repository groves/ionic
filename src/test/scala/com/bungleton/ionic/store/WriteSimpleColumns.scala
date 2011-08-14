package com.bungleton.ionic.store

import org.scalatest.testng.TestNGSuite
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

object WriteSimpleColumns {
  def makeSchema (defs :List[Tuple2[String, Schema.Type]]) :Schema = {
    val rec = Schema.createRecord("testrec", "", "", false)
    rec.setFields(defs.map(f => new Schema.Field(f._1, Schema.create(f._2), "", null)))
    rec
  }
}

class WriteSimpleColumns extends TestNGSuite {
  def check[T] (defs :List[Tuple3[String, Schema.Type, List[T]]], enc :((Encoder, T) => Unit),
    dec :((Decoder, List[T]) => Unit)) {

    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    0 until defs(0)._3.length foreach(x => defs.foreach(f => enc(encoder, f._3(x))))

    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    val root = Paths.makeMemoryFs()
    val rec = WriteSimpleColumns.makeSchema(defs.map(t => (t._1, t._2)))
    val writer = new EntryWriter(rec, root)
    0 until defs(0)._3.length foreach (_ => writer.write(decoder))
    writer.close()
    writer.close() // Additional closes should be no-ops

    defs.foreach { case (name, _, values) => {
      val in = root.open(name).read()
      val decoder = DecoderFactory.get().binaryDecoder(in, null)
      dec(decoder, values)
      assertEquals(in.read(), -1)
    }}
  }

  @Test def writeBooleans () {
    check[Boolean](List(("bool1", BOOLEAN, List(true)), ("bool2", BOOLEAN, List(false))),
      (enc, value) => enc.writeBoolean(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readBoolean(), x)))
  }

  @Test def writeInts () {
    check[Int](List(("int1", INT, List(0, 0, 0)),
      ("int2", INT, List(1, 2, 3)),
      ("int3", INT, List(101010, 10101010, 1010101010))),
      (enc, value) => enc.writeInt(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readInt(), x)))
  }

  @Test def writeSortedLong () {
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

  @Test def writeString () {
    check[String](List(("str", STRING, List("one", "two", "three"))),
      (enc, value) => enc.writeString(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readString(null).toString, x)))
  }

  @Test def writeDouble () {
    check[Double](List(("double", DOUBLE, List(0.1, 1289213.3281 -213232123.1))),
      (enc, value) => enc.writeDouble(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readDouble(), x)))
  }

  @Test def writeLong () {
    check[Long](List(("long", LONG, List(1234213123, 0, -848342834813232123l))),
      (enc, value) => enc.writeLong(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readLong(), x)))
  }

  @Test def writeFloat () {
    check[Float](List(("float", FLOAT, List(0.1f, 1289213.3281f, -2132.1f))),
      (enc, value) => enc.writeFloat(value),
      (dec, values) => values.foreach(x => assertEquals(dec.readFloat(), x)))
  }

  @Test def writeNull () {
    check[Any](List(("null", NULL, List(()))),
      (_, _) => (),
      (_, _) => ())
  }

  @Test def writeBytes () {
    check[Array[Byte]](List(("bytes", BYTES,
      List(Array[Byte](1, 2, 3), Array[Byte](), Array[Byte](23, 43, 12)))),
      (enc, value) => enc.writeBytes(value),
      (dec, values) => values.foreach(x => {
          val read = dec.readBytes(null)
          assertEquals(read.limit, x.length)
          (0 until read.limit).foreach(i => assertEquals(read.get(), x(i)))
        }))
  }
}
