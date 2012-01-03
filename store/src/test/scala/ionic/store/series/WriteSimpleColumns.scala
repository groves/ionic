package ionic.store.series

import java.io.ByteArrayOutputStream

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.io.Decoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.Encoder
import org.apache.avro.io.EncoderFactory

import org.scalatest.FunSuite

import com.threerings.fisy.Paths

object WriteSimpleColumns {
  def makeSchema(defs: List[Tuple2[String, Schema.Type]]): Schema = {
    val rec = Schema.createRecord("Simple", "", "ionic", false)
    rec.setFields(defs.map(f => new Schema.Field(f._1, Schema.create(f._2), "", null)))
    rec
  }
}

class WriteSimpleColumns extends FunSuite {
  def check[T](defs: List[Tuple3[String, Schema.Type, List[T]]], enc: ((Encoder, T) => Unit),
    dec: ((Decoder, List[T]) => Unit)) {

    val baos = new ByteArrayOutputStream
    val encoder = EncoderFactory.get().directBinaryEncoder(baos, null)
    0 until defs(0)._3.length foreach (x => defs.foreach(f => enc(encoder, f._3(x))))

    val decoder = DecoderFactory.get().binaryDecoder(baos.toByteArray(), null)
    val root = Paths.makeMemoryFs()
    val rec = WriteSimpleColumns.makeSchema(defs.map(t => (t._1, t._2)))
    val writer = new SplitSeriesWriter(rec, root)
    0 until defs(0)._3.length foreach (_ => writer.write(decoder))
    writer.close()
    writer.close() // Additional closes should be no-ops

    defs.foreach {
      case (name, _, values) => {
        val in = writer.dest.open(name).read()
        val decoder = DecoderFactory.get().binaryDecoder(in, null)
        dec(decoder, values)
        assert(in.read() === -1)
      }
    }
  }

  test("write booleans") {
    check[Boolean](List(("bool1", BOOLEAN, List(true)), ("bool2", BOOLEAN, List(false))),
      (enc, value) => enc.writeBoolean(value),
      (dec, values) => values.foreach(x => assert(dec.readBoolean() === x)))
  }

  test("write ints") {
    check[Int](List(("int1", INT, List(0, 0, 0)),
      ("int2", INT, List(1, 2, 3)),
      ("int3", INT, List(101010, 10101010, 1010101010))),
      (enc, value) => enc.writeInt(value),
      (dec, values) => values.foreach(x => assert(dec.readInt() === x)))
  }

  test("write timestamps") {
    check[Long](List(("timestamp", LONG,
      List(1000, 1000, 1000, 1000, 1001, 1001, 1001, 1002, 2002, 2002))),
      (enc, value) => enc.writeLong(value),
      (dec, values) => {
        assert(dec.readLong() === 1000)
        assert(dec.readLong() === 4)
        assert(dec.readLong() === 1)
        assert(dec.readLong() === 3)
        assert(dec.readLong() === 1)
        assert(dec.readLong() === 1)
        assert(dec.readLong() === 1000)
        assert(dec.readLong() === 2)
      })
  }

  test("write strings") {
    check[String](List(("str", STRING, List("one", "two", "three"))),
      (enc, value) => enc.writeString(value),
      (dec, values) => values.foreach(x => assert(dec.readString(null).toString === x)))
  }

  test("write doubles") {
    check[Double](List(("double", DOUBLE, List(0.1, 1289213.3281 - 213232123.1))),
      (enc, value) => enc.writeDouble(value),
      (dec, values) => values.foreach(x => assert(dec.readDouble() === x)))
  }

  test("write longs") {
    check[Long](List(("long", LONG, List(1234213123, 0, -848342834813232123l))),
      (enc, value) => enc.writeLong(value),
      (dec, values) => values.foreach(x => assert(dec.readLong() === x)))
  }

  test("write floats") {
    check[Float](List(("float", FLOAT, List(0.1f, 1289213.3281f, -2132.1f))),
      (enc, value) => enc.writeFloat(value),
      (dec, values) => values.foreach(x => assert(dec.readFloat() === x)))
  }

  test("write nulls") {
    check[Any](List(("null", NULL, List(()))),
      (_, _) => (),
      (_, _) => ())
  }

  test("write bytes") {
    check[Array[Byte]](List(("bytes", BYTES,
      List(Array[Byte](1, 2, 3), Array[Byte](), Array[Byte](23, 43, 12)))),
      (enc, value) => enc.writeBytes(value),
      (dec, values) => values.foreach(x => {
        val read = dec.readBytes(null)
        assert(read.limit === x.length)
        (0 until read.limit).foreach(i => assert(read.get() === x(i)))
      }))
  }
}
