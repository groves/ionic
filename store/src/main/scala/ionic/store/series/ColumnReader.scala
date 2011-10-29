package ionic.store.series

import org.apache.avro.generic.IndexedRecord

trait ColumnReader {
  /**
   * Reads a value from this column into IndexedRecord. Returns the number of records that had to be
   * read to find a value after skipping skip values that passed the column's filters. Returns None
   * if no such value remained in the column. The behavior if read is called again after None is returned  is undefined.
   */
  def read(rec: IndexedRecord, skip: Long): Option[Long]
  def close()
}
