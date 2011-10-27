package ionic.store.series

import java.io.ByteArrayOutputStream
import java.util.UUID

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory

import org.scalatest.FunSuite

import com.threerings.fisy.Directory
import com.threerings.fisy.Paths

object ReadSimpleColumns {
  def write(schema: Schema, numEntries: Int, enc: ((Encoder) => Unit)): Directory = {
    writeToFs(Paths.makeMemoryFs(), schema, numEntries, enc)
  }

  def writeToFs(fs: Directory, schema: Schema, numEntries: Int, enc: ((Encoder) => Unit)): Directory = {
    val seriesDir = fs.navigate(SplitSeriesReader.dir(schema) + "/" + UUID.randomUUID().toString())
    val baos = new ByteArrayOutputStream
    enc(EncoderFactory.get().directBinaryEncoder(baos, null))
    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    val writer = new SplitSeriesWriter(schema, seriesDir)
    0 until numEntries foreach (_ => writer.write(decoder))
    writer.close()
    seriesDir
  }
}

class ReadSimpleColumns extends FunSuite {
  test("reading booleans") {
    val schema = WriteSimpleColumns.makeSchema(List(("bool1", BOOLEAN)))

    val root = ReadSimpleColumns.write(schema, 3, (encoder) => {
      encoder.writeBoolean(true)
      encoder.writeBoolean(false)
      encoder.writeBoolean(false)
    })

    val reader = new SplitSeriesReader(root)
    assert(reader.next().get("bool1") === true)
    assert(reader.next().get("bool1") === false)
    assert(reader.next().get("bool1") === false)
    assert(!reader.hasNext())
    reader.close()
  }

  test("reading longs and strings") {
    val schema =
      WriteSimpleColumns.makeSchema(List(("long1", LONG), ("string2", STRING)))

    val root = ReadSimpleColumns.write(schema, 2, (encoder) => {
      encoder.writeLong(1234)
      encoder.writeString("Hi")
      encoder.writeLong(-4321)
      encoder.writeString("Bye")
    })

    val reader = new SplitSeriesReader(root)
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

    val root = ReadSimpleColumns.write(schema, 4, (encoder) => {
      encoder.writeLong(1234)
      encoder.writeString("Hi")
      encoder.writeLong(1234)
      encoder.writeString("Bye")
      encoder.writeLong(1236)
      encoder.writeString("Hi again")
      encoder.writeLong(1236)
      encoder.writeString("Bye again")
    })

    val reader = new SplitSeriesReader(root)
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

}
