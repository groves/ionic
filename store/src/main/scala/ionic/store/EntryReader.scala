package ionic.store

import scala.collection.IterableView
import scala.collection.JavaConversions._

import ionic.store.series.SplitSeriesReader
import ionic.store.series.UnitedSeriesReader

import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

class EntryReader(name: String, root: Directory) extends Iterable[GenericRecord] {
  def iterator(): Iterator[GenericRecord] = {
    (create(SplitSeriesReader.dir(name), new SplitSeriesReader(_)) ++
      create(UnitedSeriesReader.dir(name), new UnitedSeriesReader(_))).iterator
  }

  private def create(path: String, mapper: (Directory => Iterator[GenericRecord])): IterableView[GenericRecord, Iterable[_]] =
    root.navigate(path).view.collect({ case d: Directory => d }).flatMap(mapper)
}
