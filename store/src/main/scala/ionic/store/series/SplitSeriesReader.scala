package ionic.store.series

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord

import com.threerings.fisy.Directory

import ionic.query.Where
import ionic.query.LongCond
import ionic.query.LongEquals

object SplitSeriesReader {
  val prefix = "split"

  def dir(schema: Schema): String = dir(schema.getFullName)
  def dir(name: String): String = prefix + "/" + name
}

object NoneFound extends Exception {}

class SplitSeriesReader(source: Directory, where: Where = Where())
  extends Iterator[GenericRecord] {
  private val schema = SeriesReader.readSchema(source)
  private val meta = SeriesReader.readMeta(source)
  private val readers = schema.getFields.map(f =>
    if (f.schema.getType == Schema.Type.LONG) {
      val conds = where.clauses.filter(_.f == f.name).collect({ case l: LongCond => l })
      if (f.name == "timestamp") {
        new SortedLongColumnReader(source, f, meta.entries, conds)
      } else {
        new LongAvroColumnReader(source, f, conds)
      }
    } else {
      new PassthroughAvroColumnReader(source, f)
    })
  private var _read = 0L

  def hasNext(): Boolean =
    { _read != meta.entries }
  def next(): GenericRecord = {
    assert(hasNext())
    read()
  }
  def read(old: GenericRecord = null): GenericRecord = {
    val record = if (old != null) { old } else { new GenericData.Record(schema) }
    var positions = scala.collection.mutable.IndexedSeq.fill(readers.size)(-1L)
    var initialPosition = 0L
    var readPosition = 0L
    do {
      initialPosition = readPosition
      (0 until readers.size).takeWhile(_ => initialPosition == readPosition).
        filter(positions(_) != readPosition).foreach({ idx: Int =>
          readers(idx).read(record, readPosition - positions(idx) - 1) match {
            case None => throw NoneFound
            case Some(read) => {
              if (read > 1) readPosition += read - 1
              positions(idx) = readPosition
            }
          }
        })
    } while (initialPosition != readPosition)
    _read += readPosition + 1
    record
  }
  def close() { readers.foreach(_.close()) }
}
