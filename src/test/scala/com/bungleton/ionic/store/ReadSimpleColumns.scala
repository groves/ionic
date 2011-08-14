package com.bungleton.ionic.store

import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.ByteArrayOutputStream
import org.apache.avro.generic.GenericData
import com.threerings.fisy.Paths
import scala.collection.mutable.Stack
import org.scalatest.FunSuite
import org.apache.avro.Schema.Type._

class ReadSimpleColumns extends FunSuite {
  test("reading booleans") {
    val root = Paths.makeMemoryFs()
    val schema = WriteSimpleColumns.makeSchema(List(("bool1", BOOLEAN)))

    val toWrite = new GenericData.Record(schema)
    toWrite.put("bool1", true)
    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    encoder.writeBoolean(true)
    val writer = new EntryWriter(schema, root)
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    writer.write(decoder)
    writer.close()

    val reader = new EntryReader(root)
    val toRead = new GenericData.Record(schema)
    toRead.put("bool1", true)
    reader.read(toRead)
    assert(toRead.get("bool1") == true)
    reader.close()
  }
}
