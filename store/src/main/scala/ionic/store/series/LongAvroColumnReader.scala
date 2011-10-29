package ionic.store.series

import ionic.query.LongCond
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.Schema
import com.threerings.fisy.Directory

class LongAvroColumnReader(source: Directory, field: Schema.Field, conds: Iterable[LongCond])
  extends AvroColumnReader(source, field) {
  def read(rec: IndexedRecord, skip: Long) = {
    (0L until skip).foreach({ _ => decoder.readLong() })
    var read = 0L
    var value = 0L
    do {
      value = decoder.readLong()
      read += 1
    } while (conds.exists(!_.meets(value)))
    rec.put(field.pos, value)
    Some(read)
  }

}
