package ionic.store.series

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
  closedWriters: Iterable[Directory]) extends Iterable[GenericRecord] {
  val name = query.from
  // TODO - thread safety of written
  val openPositions: Iterable[Tuple2[Directory, Int]] = openWriters.map((w: UnitedSeriesWriter) => {
    (w.dest, w.written)
  })
  // TODO - splits
  def iterator(): Iterator[GenericRecord] = {
    (closedWriters.flatMap(new UnitedSeriesReader(_, query.where)) ++
      openPositions.flatMap((t: Tuple2[Directory, Int]) => {
        new UnitedSeriesReader(t._1, query.where, t._2)
      })).iterator
  }
}
class SeriesParceler(base: LocalDirectory, name: String) {
  import ionic.util.ReactImplicits._
  // TODO - validate existing United and transfer to unfilled Split

  val openWriters = new HashSet[UnitedSeriesWriter]
  val closedWriters: Buffer[Directory] =
    base.navigate(UnitedSeriesReader.dir(name)).collect({ case d: Directory => d }).toBuffer

  def reader(clauses: String = ""): Iterable[GenericRecord] =
    new ParceledReader(Query.parse(name), base, openWriters, closedWriters)

  def writer(schema: Schema): UnitedSeriesWriter = {
    val writer = new UnitedSeriesWriter(schema, base)
    writer.closed.connect(() => {
      openWriters.remove(writer)
      closedWriters += writer.dest
    })
    openWriters.add(writer)
    writer
  }
}
