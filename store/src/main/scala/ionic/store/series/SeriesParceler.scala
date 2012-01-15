package ionic.store.series

import com.google.common.collect.HashMultiset
import com.google.common.collect.Multiset
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

class SeriesParceler(val base: LocalDirectory, name: String) extends Logging {
  import ionic.util.ReactImplicits._

  private val splitter :Executor = Executors.newSingleThreadExecutor()

  // open series writers, readers on those writers, and fully-transferred splits. All access to
  // these variables post-construction must be inside synchronization on writers.
  private val writers = new HashSet[UnitedSeriesWriter]
  private val openUnited :Multiset[Directory] = HashMultiset.create()
  private val splits: Buffer[Directory] =
    base.navigate(Series.splitPrefix + "/" + name).collect({ case d: Directory => d }).toBuffer

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
      splits -= split.source
      split.source.delete()
      if (united.entries > 0) {
        log.warn("Redoing incomplete transfer")
        splits += SplitSeriesWriter.transferFrom(base, united).dest
      } else {
        log.warn("Incomplete transfer was of empty united, deleting it as well",
          "united", unitedDir)
      }
      unitedDir.delete()
    }
  })
  base.navigate(Series.unitedPrefix + "/" + name).collect({ case d: Directory => d }).map(
    new UnitedSeriesReader(_)).foreach((reader :UnitedSeriesReader) => {
      splits += SplitSeriesWriter.transferFrom(base, reader).dest
      reader.source.delete()
    })

  private def readers(query :Query) :Iterable[Iterator[GenericRecord]] = {
    writers synchronized {
      val openPositions: Iterable[Tuple2[Directory, Long]] =
        writers.map((w: UnitedSeriesWriter) => { (w.dest, w.written) })
      (splits.map(new SplitSeriesReader(_, query.where)) ++
        openPositions.map((t: Tuple2[Directory, Long]) => {
          openUnited.add(t._1)
          new UnitedSeriesReader(t._1, query.where, t._2)
        }))
    }
  }
  private def release(iterators :Iterable[Iterator[GenericRecord]]) {
    writers synchronized {
      iterators.collect({case i: UnitedSeriesReader => i}).foreach(
        (reader :UnitedSeriesReader) => {
          openUnited.remove(reader.source)
          if (!openUnited.contains(reader.source)) reader.source.delete()
          })
      }
  }
  def reader(clauses: String = "") = new CloseableIterable[GenericRecord] {
    val query = Query.parse(name)
    def iterator() = new CloseableIterator[GenericRecord] {
      var closed = false
      val iterators = readers(query)
      val flattened = iterators.iterator.flatten
      def hasNext :Boolean = if (!flattened.hasNext) {
        close()
        return false
      } else return true
      def next =  flattened.next
      def close = if (!closed) {
          closed = true
          release(iterators)
        }
    }
  }

  def writer(schema: Schema): UnitedSeriesWriter = {
    val writer = new UnitedSeriesWriter(schema, base)
    writer.closed.connect(() => {
      // TODO - thread safety of writer transfer
      splitter.execute(new Runnable() {
        def run {
          val split = SplitSeriesWriter.transferFrom(base, writer)
          writers synchronized {
            splits += split.dest
            writers -= writer
            if (!openUnited.contains(writer.dest)) writer.dest.delete()
          }
        }})
    })
    writers synchronized { writers += writer }
    writer
  }
}
