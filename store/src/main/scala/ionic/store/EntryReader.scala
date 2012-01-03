package ionic.store

import ionic.store.series.Series
import scala.collection.IterableView
import scala.collection.JavaConversions._

import ionic.query.Query
import ionic.store.series.SplitSeriesReader
import ionic.store.series.UnitedSeriesReader

import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

object EntryReader {
  def apply(name: String, root: Directory): EntryReader = EntryReader(Query.parse(name), root)
  def apply(query: Query, root: Directory): EntryReader = new EntryReader(query, root)
}

// TODO - return a subtrait of Iterable with close on it
class EntryReader(query: Query, root: Directory) extends Iterable[GenericRecord] {
  val name = query.from
  def iterator(): Iterator[GenericRecord] = {
    (create(Series.splitPrefix, new SplitSeriesReader(_, query.where)) ++
      create(Series.unitedPrefix, new UnitedSeriesReader(_, query.where))).iterator
  }

  private def create(prefix: String, mapper: (Directory => Iterator[GenericRecord])): IterableView[GenericRecord, Iterable[_]] =
    root.navigate(prefix + "/" + name).view.collect({ case d: Directory => d }).flatMap(mapper)
}
