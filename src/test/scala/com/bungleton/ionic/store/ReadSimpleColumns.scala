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

    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    encoder.writeBoolean(true)
    encoder.writeBoolean(false)
    encoder.writeBoolean(false)
    val writer = new EntryWriter(schema, root)
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    writer.write(decoder)
    writer.write(decoder)
    writer.write(decoder)
    writer.close()

    val reader = new EntryReader(root)
    val toRead = new GenericData.Record(schema)
    reader.read(toRead)
    assert(toRead.get("bool1") === true)
    reader.read(toRead)
    assert(toRead.get("bool1") === false)
    reader.read(toRead)
    assert(toRead.get("bool1") === false)
    reader.close()
  }

  test("reading longs and strings") {
    val root = Paths.makeMemoryFs()
    val schema =
        WriteSimpleColumns.makeSchema(List(("long1", LONG), ("string2", STRING)))

    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    encoder.writeLong(1234)
    encoder.writeString("Hi")
    encoder.writeLong(-4321)
    encoder.writeString("Bye")
    val writer = new EntryWriter(schema, root)
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    writer.write(decoder)
    writer.write(decoder)
    writer.close()

    val reader = new EntryReader(root)
    val toRead = new GenericData.Record(schema)
    reader.read(toRead)
    assert(toRead.get("long1") === 1234)
    assert(toRead.get("string2").toString() === "Hi")
    reader.read(toRead)
    assert(toRead.get("long1") === -4321)
    assert(toRead.get("string2").toString() === "Bye")
    reader.close()
  }

  test("reading timestamps") {
    val root = Paths.makeMemoryFs()
    val schema =
      WriteSimpleColumns.makeSchema(List(("timestamp", LONG), ("string", STRING)))

    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    encoder.writeLong(1234)
    encoder.writeString("Hi")
    encoder.writeLong(1234)
    encoder.writeString("Bye")
    encoder.writeLong(1236)
    encoder.writeString("Hi again")
    encoder.writeLong(1236)
    encoder.writeString("Bye again")
    val writer = new EntryWriter(schema, root)
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    println(baos.toByteArray().length)
    writer.write(decoder)
    writer.write(decoder)
    writer.write(decoder)
    writer.write(decoder)
    writer.close()

    val reader = new EntryReader(root)
    val toRead = new GenericData.Record(schema)
    reader.read(toRead)
    assert(toRead.get("timestamp") === 1234)
    assert(toRead.get("string").toString() === "Hi")
    reader.read(toRead)
    assert(toRead.get("timestamp") === 1234)
    assert(toRead.get("string").toString() === "Bye")
    reader.read(toRead)
    assert(toRead.get("timestamp") === 1236)
    assert(toRead.get("string").toString() === "Hi again")
    reader.read(toRead)
    assert(toRead.get("timestamp") === 1236)
    assert(toRead.get("string").toString() === "Bye again")
    reader.close()
  }
}
