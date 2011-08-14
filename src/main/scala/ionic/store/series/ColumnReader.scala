package ionic.store.series

import org.apache.avro.generic.IndexedRecord

trait ColumnReader {
  def read(rec: IndexedRecord)
  def close()
}
