package ionic.store

import java.lang.Iterable
import java.util.Iterator

import scala.collection.JavaConversions._

import com.google.common.collect.Iterators

import ionic.store.series.SeriesReader

import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

class EntryReader(name: String, root: Directory) extends Iterable[GenericRecord] {
  def iterator(): Iterator[GenericRecord] = {
    // Use view to keep from creating the readers until they're used
    val readers: Iterator[Iterator[GenericRecord]] =
      root.navigate(name).view.collect({ case d: Directory => d }).map(new SeriesReader(_)).iterator
    Iterators.concat(readers)
  }
}
