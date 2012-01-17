package ionic.store.series

import java.io.InputStream
import scala.collection.JavaConversions._

import ionic.query.LongCond
import ionic.query.NumCond
import ionic.query.Where

import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.Directory
import org.xerial.snappy.SnappyInputStream

class SplitSeriesReader(val source: Directory, where: Where = Where()) extends LookaheadReader {
  private val schema = SeriesReader.readSchema(source)
  val meta = SeriesReader.readMeta(source)

  private def createPrimitiveReader(f: Schema.Field, entries: Long, reader: AvroPrimitiveReader , inWrapper:(InputStream) => InputStream=(in :InputStream) => { in }) = {
    val in = inWrapper(source.open(f.name).read())
    new AvroPrimitiveColumnReader(DecoderFactory.get().binaryDecoder(in, null), f, entries,
      reader, () => { in.close() })
  }
  // Create a list of readers for each field with the conditions in the where for that field
  private val readers = schema.getFields.map(f => {
    val fClauses = where.clauses.filter(_.f == f.name)
    if (f.schema.getType == LONG) {
      // TODO - freak out if there are non-LongConds
      val conds = fClauses.collect({ case n: NumCond => n }).map(_.toLong)
      if (f.name == "timestamp") {
        new SortedLongColumnReader(source, f, meta.entries, conds)
      } else {
          createPrimitiveReader(f, meta.entries, new AvroLongReader(conds))
      }
    } else if (f.schema.getType == STRING) {
      createPrimitiveReader(f, meta.entries, AvroPrimitiveReader(f.schema.getType, fClauses),
        (in :InputStream) => { new SnappyInputStream(in) })
    } else {
    createPrimitiveReader(f, meta.entries, AvroPrimitiveReader(f.schema.getType, fClauses))
    }
  })

  def read(old: GenericRecord = null): GenericRecord = {
    val record = if (old != null) { old } else { new GenericData.Record(schema) }
    // Start with all the readers at the current position
    var positions = scala.collection.mutable.IndexedSeq.fill(readers.size)(-1L)
    var doPosition = 0L // The position at the start of the do loop
    var readPosition = 0L // Records read in this call to read
    do {
      // Go through each of the readers not at readPosition and read until a matching value for
      // all fields is found.
      doPosition = readPosition
      (0 until readers.size).takeWhile(_ => doPosition == readPosition).
        filter(positions(_) != readPosition).foreach({ idx: Int =>
          // Skip up from the reader's position to readPosition and read the next match into record
          readers(idx).read(record, readPosition - positions(idx) - 1) match {
            case None => throw NoneFound // Off the end with no match
            case Some(read) => {
              // Move up for the number of records read to the match. If we skip, bump
              // readPosition to get the other readers to catch up.
              if (read > 1) readPosition += read - 1
              positions(idx) = readPosition
            }
          }
        })
    } while (doPosition != readPosition)
    record // Made it all the way through for all the readers, record is filled with the values
  }
  def close() { readers.foreach(_.close()) }
}
