package ionic.store

import ionic.query.Query
import ionic.store.series.CloseableIterable
import ionic.store.series.SeriesParceler

import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.impl.local.LocalDirectory

object EntryReader {
  def apply(name: String, root: LocalDirectory): CloseableIterable[GenericRecord] =
    EntryReader(Query.parse(name), root)
  def apply(query: Query, root: LocalDirectory): CloseableIterable[GenericRecord] =
    new SeriesParceler(root, query.from).reader(query)
}
