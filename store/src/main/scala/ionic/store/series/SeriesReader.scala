package ionic.store.series

import scala.collection.JavaConversions._

import org.apache.avro.Schema
import org.apache.avro.io.DatumReader
import org.apache.avro.io.DecoderFactory
import org.apache.avro.specific.SpecificDatumReader

import com.threerings.fisy.Directory

object SeriesReader {
  def readSchema(source: Directory) = Schema.parse(source.open("schema.avsc").read())
  def readMeta(source: Directory) = {
    val in = source.open("meta.avsc").read()
    val decoder = DecoderFactory.get().jsonDecoder(SeriesMetadata.SCHEMA$, in)
    val metaReader: DatumReader[SeriesMetadata] = new SpecificDatumReader(SeriesMetadata.SCHEMA$)
    metaReader.read(new SeriesMetadata(), decoder)
  }
}
