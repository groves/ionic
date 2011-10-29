package ionic.store.series

import ionic.query.LongCond
import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.Directory

class SortedLongColumnReader(source: Directory, field: Schema.Field, var entries: Long, conds: Iterable[LongCond])
  extends ColumnReader {
  private val in = source.open(field.name).read()
  private val decoder = DecoderFactory.get().binaryDecoder(in, null)

  private var value: Long = 0
  private var countAtValue: Long = 0
  private var closed: Boolean = false

  def read(rec: IndexedRecord, skip: Long): Option[Long] = {
    assume(entries > 0)
    var tillFirst = skip + 1 // Read one past the skip to get to the value we want
    while (tillFirst > countAtValue) {
      tillFirst -= countAtValue
      entries -= countAtValue
      if (entries == 0) return None
      value += decoder.readLong()
      countAtValue = decoder.readLong()
    }
    countAtValue -= tillFirst
    entries -= tillFirst
    var read = 1L
    while (!conds.forall(_.meets(value))) {
      read += countAtValue
      value += decoder.readLong()
      entries -= countAtValue
      if (entries == 0) return None
      countAtValue = decoder.readLong()
    }
    rec.put(field.pos, value)
    Some(read)
  }

  def close() { in.close() }
}
