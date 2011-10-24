package ionic.query

import ionic.store.EntryReader

import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

class QueryRunner(query: String, root: Directory)
  extends Iterable[GenericRecord] {
  val parsed: Query = new IQLParser().parse(query)

  def iterator(): Iterator[GenericRecord] = {
    new EntryReader(parsed.from, root).iterator
  }
}
