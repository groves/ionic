package com.bungleton.ionic.store

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

class WriteSimpleColumns {
  def encodeThenWrite (rec :Schema, records :Int, enc :(Encoder => Unit)) :Directory= {
    val baos = new ByteArrayOutputStream
    enc(EncoderFactory.get().directBinaryEncoder(baos, null))
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    val root = Paths.makeMemoryFs()
    val writer = new Writer(rec, decoder, root)
    0 until records foreach (_ => writer.write)
    root
  }

  def createRecord (name :String, fields :List[Schema.Field]) = {
    val record = Schema.createRecord(name, "", "", false)
    record.setFields(fields)
    record
  }

  def createField (name :String, stype :Schema.Type) = {
    new Schema.Field(name, Schema.create(stype), "", null)
  }

  @Test def writeBooleans () = {
    val fields = List("bool1", "bool2")
    val record = createRecord("TestBooleans", fields.map(createField(_, Schema.Type.BOOLEAN)))

    val root = encodeThenWrite(record, 1, enc => {
      enc.writeBoolean(true);
      enc.writeBoolean(false)
    })

    fields.zip(List(1, 0)).foreach { case (name, value) => {
      val in = root.open(name).read()
      assertEquals(in.read(), value, name + " wrote the wrong value")
      assertEquals(in.read(), -1)
    }}
  }

  @Test def writeInts () = {
    val fields = List("int1", "int2", "int3")
    val record = createRecord("TestInts", fields.map(createField(_, Schema.Type.INT)))

    val colValues = List(List(0, 0, 0), List(1, 2, 3), List(101010, 10101010, 1010101010))
    val root = encodeThenWrite(record, 3, enc => {
      0 until 3 foreach(x => colValues.foreach(vals => enc.writeInt(vals(x))))
    })
    fields.zip(colValues).foreach { case (name, values) => {
      val in = root.open(name).read()
      val dec = DecoderFactory.get().binaryDecoder(in, null)
      values.foreach(v => assertEquals(dec.readInt(), v, name + " wrote the wrong value"))
      assertEquals(in.read(), -1)
    }}
  }
}
