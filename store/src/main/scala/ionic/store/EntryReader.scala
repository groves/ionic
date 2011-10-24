package ionic.store

import scala.collection.IterableView
import scala.collection.JavaConversions._

import ionic.store.series.ImmediateSeriesReader
import ionic.store.series.SeriesReader

import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

class EntryReader(name: String, root: Directory) extends Iterable[GenericRecord] {
  def iterator(): Iterator[GenericRecord] = {
    (create(name, new SeriesReader(_)) ++
      create("live/" + name, new ImmediateSeriesReader(_))).iterator
  }

  private def create(path: String, mapper: (Directory => Iterator[GenericRecord])): IterableView[GenericRecord, Iterable[_]] =
    root.navigate(path).view.collect({ case d: Directory => d }).map(mapper).flatMap(r => r)
}
