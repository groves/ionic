package com.bungleton.ionic.store

import org.testng.Assert._
import com.threerings.fisy.Paths;
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import org.apache.avro.io.DecoderFactory
import org.apache.avro.Schema
import org.testng.annotations.Test

import scala.collection.JavaConversions._

class WriteBooleanColumns {
  @Test def write() = {
    val record = Schema.createRecord("TestBooleans", "", "", false)
    val fields = List("bool1", "bool2")
    record.setFields(fields.map(new Schema.Field(_, Schema.create(Schema.Type.BOOLEAN), "", null)))

    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    encoder.writeBoolean(true)
    encoder.writeBoolean(false)

    val root = Paths.makeMemoryFs()
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    val writer = new Writer(record, decoder, root)
    writer.write()

    fields.zip(List(1, 0)).foreach { case (name, value) => {
      val in = root.open(name).read()
      assertEquals(in.read(), value, name + " wrote the wrong value")
      assertEquals(in.read(), -1)
    }}
  }
}
