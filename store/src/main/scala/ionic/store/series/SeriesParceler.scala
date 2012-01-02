package ionic.store.series

import ionic.store.EntryReader
import org.apache.avro.Schema
import scala.collection.mutable.HashSet
import com.threerings.fisy.impl.local.LocalDirectory
import org.apache.avro.generic.GenericRecord

class SeriesParceler(base: LocalDirectory, name: String) {
  // TODO - validate existing United and transfer to unfilled Split

  val openWriters = new HashSet[UnitedSeriesWriter]

  def reader(clauses: String = ""): Iterable[GenericRecord] = EntryReader(name, base)
  def writer(schema: Schema): UnitedSeriesWriter = {
    val writer = new UnitedSeriesWriter(schema, base)
    openWriters.add(writer)
    writer
  }
}
