package ionic.store.series

import java.util.concurrent.Executor
import java.util.concurrent.Executors

import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashSet

import com.codahale.logula.Logging

import ionic.query.Query

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory
import com.threerings.fisy.impl.local.LocalDirectory

class ParceledReader(query: Query, root: Directory, openWriters: Iterable[UnitedSeriesWriter],
  closedWriters: Iterable[Directory], splits: Iterable[Directory]) extends Iterable[GenericRecord] {
  val name = query.from
  // TODO - thread safety of written
  val openPositions: Iterable[Tuple2[Directory, Int]] = openWriters.map((w: UnitedSeriesWriter) => {
    (w.dest, w.written)
  })
  def iterator(): Iterator[GenericRecord] = {
    (splits.flatMap(new SplitSeriesReader(_, query.where)) ++
      closedWriters.flatMap(new UnitedSeriesReader(_, query.where)) ++
      openPositions.flatMap((t: Tuple2[Directory, Int]) => {
        new UnitedSeriesReader(t._1, query.where, t._2)
      })).iterator
  }
}
class SeriesParceler(val base: LocalDirectory, name: String) extends Logging {
  import ionic.util.ReactImplicits._
  // TODO - validate existing United and transfer to unfilled Split

  private val openWriters = new HashSet[UnitedSeriesWriter]
  private val splitter :Executor = Executors.newSingleThreadExecutor()
  private def splits: Iterable[Directory] =
    base.navigate(Series.splitPrefix + "/" + name).collect({ case d: Directory => d })

  splits.map(new SplitSeriesReader(_)).filter((reader: SplitSeriesReader) => {
    base.navigate(reader.meta.transferredFrom.toString).exists()
  }).foreach((split: SplitSeriesReader) => {
    val unitedDir = base.navigate(split.meta.transferredFrom.toString)
    val united = new UnitedSeriesReader(unitedDir)
    if (united.entries == split.meta.entries) {
      log.warn("Completed united to split transfer lingers, deleting",
        "united", split.meta.transferredFrom)
      unitedDir.delete()
    } else {
      log.warn("Found incomplete transfer to split. Deleting", "split", split.source)
      split.source.delete()
      if (united.entries > 0) {
        log.warn("Redoing incomplete transfer")
        SplitSeriesWriter.transferFrom(base, united.schema, unitedDir,
          united.entries.asInstanceOf[Int])
      } else {
        log.warn("Incomplete transfer was of empty united, deleting it as well",
          "united", unitedDir)
      }
      unitedDir.delete()
    }
  })
  private val closedWriters: Buffer[Directory] =
    base.navigate(Series.unitedPrefix + "/" + name).collect({ case d: Directory => d }).toBuffer

  def reader(clauses: String = ""): Iterable[GenericRecord] =
    new ParceledReader(Query.parse(name), base, openWriters, closedWriters, splits)

  def writer(schema: Schema): UnitedSeriesWriter = {
    val writer = new UnitedSeriesWriter(schema, base)
    writer.closed.connect(() => {
      // TODO - thread safety of writer transfer
      openWriters -= writer
      closedWriters += writer.dest
      splitter.execute(new Runnable() {
        def run {
          val split: SplitSeriesWriter = SplitSeriesWriter.transferFrom(base, writer)
          assert(split.written == writer.written)
          closedWriters -= writer.dest
          writer.dest.delete()
        }})
    })
    openWriters += writer
    writer
  }
}
