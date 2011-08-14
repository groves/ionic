package ionic.store.series

import java.io.ByteArrayOutputStream

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory

import org.scalatest.FunSuite

import com.threerings.fisy.Directory
import com.threerings.fisy.Paths

class ReadSimpleColumns extends FunSuite {
  test("reading booleans") {
    val schema = WriteSimpleColumns.makeSchema(List(("bool1", BOOLEAN)))

    val root = write(schema, 3, (encoder) => {
      encoder.writeBoolean(true)
      encoder.writeBoolean(false)
      encoder.writeBoolean(false)
    })

    val reader = new SeriesReader(root)
    assert(reader.read().get("bool1") === true)
    assert(reader.read().get("bool1") === false)
    assert(reader.read().get("bool1") === false)
    reader.close()
  }

  test("reading longs and strings") {
    val schema =
      WriteSimpleColumns.makeSchema(List(("long1", LONG), ("string2", STRING)))

    val root = write(schema, 2, (encoder) => {
      encoder.writeLong(1234)
      encoder.writeString("Hi")
      encoder.writeLong(-4321)
      encoder.writeString("Bye")
    })

    val reader = new SeriesReader(root)
    val toRead = new GenericData.Record(schema)
    assert(reader.read(toRead).get("long1") === 1234)
    assert(toRead.get("string2").toString() === "Hi")

    assert(reader.read(toRead).get("long1") === -4321)
    assert(toRead.get("string2").toString() === "Bye")
    reader.close()
  }

  test("reading timestamps") {
    val schema =
      WriteSimpleColumns.makeSchema(List(("timestamp", LONG), ("string", STRING)))

    val root = write(schema, 4, (encoder) => {
      encoder.writeLong(1234)
      encoder.writeString("Hi")
      encoder.writeLong(1234)
      encoder.writeString("Bye")
      encoder.writeLong(1236)
      encoder.writeString("Hi again")
      encoder.writeLong(1236)
      encoder.writeString("Bye again")
    })

    val reader = new SeriesReader(root)
    val read = reader.read()
    assert(read.get("timestamp") === 1234)
    assert(read.get("string").toString() === "Hi")

    assert(reader.read(read).get("timestamp") === 1234)
    assert(read.get("string").toString() === "Bye")

    assert(reader.read(read).get("timestamp") === 1236)
    assert(read.get("string").toString() === "Hi again")

    assert(reader.read(read).get("timestamp") === 1236)
    assert(read.get("string").toString() === "Bye again")
    reader.close()
  }

  def write(schema: Schema, numEntries: Int, enc: ((Encoder) => Unit)): Directory = {
    val root = Paths.makeMemoryFs()
    val baos = new ByteArrayOutputStream
    enc(EncoderFactory.get().directBinaryEncoder(baos, null))
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    val writer = new SeriesWriter(schema, root)
    0 until numEntries foreach (_ => writer.write(decoder))
    writer.close()
    root
  }
}
