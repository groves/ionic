package ionic.store.series

import ionic.query.Query
import java.util.concurrent.CountDownLatch

import scala.collection.JavaConversions._

import com.codahale.logula.Logging

import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.apache.avro.util.ByteBufferOutputStream
import org.apache.log4j.Level

import org.scalatest.FunSuite

import com.threerings.fisy.Paths

object ParcelOps {
  import ionic.util.RunnableImplicit._
  val schema =
    WriteSimpleColumns.makeSchema(("timestamp", LONG), ("playerId", LONG), ("score", FLOAT))

  def makeParceler = new SeriesParceler(Paths.makeTempFs(), schema.getFullName)

  def makeRecord(ts: Long, playerId: Long, score: Float) = {
    val record = new GenericData.Record(schema)
    record.put("timestamp", ts)
    record.put("playerId", playerId)
    record.put("score", score)
    record
  }

  def waitForSplit (parceler :SeriesParceler) {
    val waitOn = new CountDownLatch(1)
    parceler.splitter.execute(() => { waitOn.countDown() })
    waitOn.await()
  }

  def write(parceler: SeriesParceler, values: Tuple3[Long, Long, Float]*): UnitedSeriesWriter =
    write(parceler.writer(schema), values: _*)

  def write(writer: UnitedSeriesWriter, values: Tuple3[Long, Long, Float]*): UnitedSeriesWriter = {
    val out = new ByteBufferOutputStream()
    val enc = EncoderFactory.get.directBinaryEncoder(out, null)
    val datumWriter = new SpecificDatumWriter[GenericData.Record](schema)
    values.map((makeRecord _).tupled(_)).foreach(rec => {
        datumWriter.write(rec, enc)
        out.getBufferList().foreach(writer.write(_))
      })
    writer
  }
}

class ParcelSeries extends FunSuite {
  import ParcelOps._
  Logging.configure { log => log.level = Level.INFO }

  test("read from writer transitioned to split") {
    val parceler = makeParceler
    write(parceler, (1234L, 1L, 12.7F)).close()
    assert(parceler.reader().size === 1)
    waitForSplit(parceler)
    val laterParceler = new SeriesParceler(parceler.base, schema.getFullName)
    assert(laterParceler.reader().size === 1)
  }

  test("open with completed but undeleted transfer") {
    val fs = Paths.makeTempFs()
    val writer = new UnitedSeriesWriter(schema, fs)
    write(writer, (2234L, 1L, 12.7F), (1234L, 2L, 12345.92F))
    writer.close()
    SplitSeriesWriter.transferFrom(fs, writer)
    assert(new SeriesParceler(fs, schema.getFullName).reader().size === 2)
  }

  test("open with incomplete transfer") {
    val fs = Paths.makeTempFs()
    val writer = new UnitedSeriesWriter(schema, fs)
    write(writer, (2234L, 1L, 12.7F), (1234L, 2L, 12345.92F))
    writer.close()
    // Marks as transferring without actually doing the transfer
    new SplitSeriesWriter(writer.schema, fs, writer.dest.getPath).close()
    assert(new SeriesParceler(fs, schema.getFullName).reader().size === 2)
  }

  test("open with finished united") {
    val fs = Paths.makeTempFs()
    val writer = new UnitedSeriesWriter(schema, fs)
    write(writer, (2234L, 1L, 12.7F), (1234L, 2L, 12345.92F))
    writer.close()
    assert(new SeriesParceler(fs, schema.getFullName).reader().size === 2)
  }

  test("read from open writer") {
    val parceler = makeParceler
    write(parceler, (1234L, 1L, 12.7F), (2345L, 1L, 17.6F))
    assert(parceler.reader().size === 2)
  }

  test("multiple writers in various states") {
    val parceler = makeParceler
    val openWriter = write(parceler, (1234L, 1L, 12.7F), (1235L, 2L, 10.5F), (1235L, 1L, 5F))
    write(parceler, (2234L, 1L, 12.7F), (2235L, 2L, 10.5F), (2235L, 1L, 5F)).close()
    val reader = parceler.reader()
    assert(reader.size === 6)
    write(openWriter, (4342L, 1L, -10.4F))
    assert(reader.size === 7)
    val iter = parceler.reader().iterator()
    openWriter.close()
    waitForSplit(parceler)
    assert(parceler.reader().size === 7)
    assert(openWriter.dest.exists(), "An open iter keeps a united series around")
    assert(iter.size === 7)
    assert(!openWriter.dest.exists(), "Closing the last iter deletes the series")
  }

  test("live enum series") {
    val eschema =
      WriteSimpleColumns.makeSchema(("timestamp", LONG), ("enum1", ENUM))

    val parceler = new SeriesParceler(Paths.makeTempFs(), eschema.getFullName)

    val writer = parceler.writer(eschema)
    val out = new ByteBufferOutputStream()
    val enc = EncoderFactory.get.directBinaryEncoder(out, null)
    val datumWriter = new SpecificDatumWriter[GenericData.Record](eschema)
    List((1234L, "zero"), (1235L, "three"), (1236L, "one")).map((t: Tuple2[Long, String]) => {
      val record = new GenericData.Record(eschema)
      record.put("timestamp", t._1)
      record.put("enum1", t._2)
      record
    }).foreach(rec => {
      datumWriter.write(rec, enc)
      out.getBufferList().foreach(writer.write(_))
    })
    assert(parceler.reader().size === 3)
    val reader = parceler.reader().iterator()
    assert(reader.next().get("enum1") === 0)
    assert(reader.next().get("enum1") === 3)
    assert(reader.next().get("enum1") === 1)
    assert(parceler.reader(Query.parse("ionic.Simple where enum1 = \"zero\"")).size === 1)
    writer.close()
    waitForSplit(parceler)
    assert(parceler.reader().size === 3)
    assert(parceler.reader(Query.parse("ionic.Simple where enum1 = \"zero\"")).size === 1)
  }
}
