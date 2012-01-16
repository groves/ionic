package ionic.store.series

import org.apache.avro.generic.GenericData
import scala.collection.JavaConversions._
import ionic.query.Where

import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.Directory

object UnitedSeriesReader {
  def makeDecoder(source: Directory) =
    DecoderFactory.get().binaryDecoder(source.open("series").read(), null)
}
class UnitedSeriesReader(val source: Directory, where: Where = Where(), var entries: Long = -1L)
  extends LookaheadReader {
  val schema = SeriesReader.readSchema(source)
  if (entries == -1L) {
    entries = SeriesReader.readMeta(source).entries
  }
  private val recordDecoder = UnitedSeriesReader.makeDecoder(source)

  // TODO - freak if missing field for clause
  private val readers = schema.getFields.map(f =>
    new AvroPrimitiveColumnReader(recordDecoder, f, entries,
      AvroPrimitiveReader(f.schema.getType, where.clauses.filter(_.f == f.name)))).toSeq

  def read(old: GenericRecord = null): GenericRecord = {
    val record = if (old != null) { old } else { new GenericData.Record(schema) }
    var allMatches = false
    while(!allMatches) {
      allMatches = readers.foldLeft(true)((matches, reader) => {
        if (matches) reader.readOne(record)
        else {
          reader.skip()
          false
        }
      })
      if (readers.head.entries <= 0 && !allMatches) throw NoneFound
    }
    record

  }
  def close() { recordDecoder.inputStream.close() }

}
