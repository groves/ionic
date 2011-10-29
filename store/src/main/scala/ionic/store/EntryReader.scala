package ionic.store

import ionic.query.Query
import scala.collection.IterableView
import scala.collection.JavaConversions._

import ionic.store.series.SplitSeriesReader
import ionic.store.series.UnitedSeriesReader

import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

object EntryReader {
  def apply(name: String, root: Directory): EntryReader = EntryReader(Query.parse(name), root)
  def apply(query: Query, root: Directory): EntryReader = new EntryReader(query, root)
}

class EntryReader(query: Query, root: Directory) extends Iterable[GenericRecord] {
  val name = query.from
  def iterator(): Iterator[GenericRecord] = {
    (create(SplitSeriesReader.dir(name), new SplitSeriesReader(_, query.where)) ++
      create(UnitedSeriesReader.dir(name), new UnitedSeriesReader(_, query.where))).iterator
  }

  private def create(path: String, mapper: (Directory => Iterator[GenericRecord])): IterableView[GenericRecord, Iterable[_]] =
    root.navigate(path).view.collect({ case d: Directory => d }).flatMap(mapper)
}
