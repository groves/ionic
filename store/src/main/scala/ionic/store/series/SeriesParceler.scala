package ionic.store.series

import org.apache.avro.io.DecoderFactory
import ionic.query.Query
import scala.collection.IterableView
import scala.collection.JavaConversions._

import com.threerings.fisy.Directory
import scala.collection.mutable.Buffer
import ionic.store.EntryReader
import org.apache.avro.Schema
import scala.collection.mutable.HashSet
import com.threerings.fisy.impl.local.LocalDirectory
import org.apache.avro.generic.GenericRecord

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
class SeriesParceler(val base: LocalDirectory, name: String) {
  import ionic.util.ReactImplicits._
  // TODO - validate existing United and transfer to unfilled Split

  private val openWriters = new HashSet[UnitedSeriesWriter]
  private val closedWriters: Buffer[Directory] =
    base.navigate(Series.unitedPrefix + "/" + name).collect({ case d: Directory => d }).toBuffer

  private def splits: Iterable[Directory] =
    base.navigate(Series.splitPrefix + "/" + name).collect({ case d: Directory => d })

  def reader(clauses: String = ""): Iterable[GenericRecord] =
    new ParceledReader(Query.parse(name), base, openWriters, closedWriters, splits)

  def writer(schema: Schema): UnitedSeriesWriter = {
    val writer = new UnitedSeriesWriter(schema, base)
    writer.closed.connect(() => {
      openWriters.remove(writer)
      // TODO - put the transfer on a background thread, note transfer on fs
      //closedWriters += writer.dest
      val split = new SplitSeriesWriter(writer.schema, base, writer.dest.getPath)
      writer.startTransfer(split.dest.getPath)
      val decoder = DecoderFactory.get().binaryDecoder(writer.dest.open("series").read(), null)
      (1 to writer.written).foreach(_ => { split.write(decoder) })
      split.close()
      assert(split.written == writer.written)
      writer.dest.delete()
    })
    openWriters.add(writer)
    writer
  }
}
