package ionic.store.series

import java.io.OutputStreamWriter
import java.util.UUID

import scala.collection.JavaConversions._

import com.google.common.base.Charsets

import org.apache.avro.Schema
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import com.threerings.fisy.Directory

object SeriesWriter {
  def genDir(prefix: String, name: String) = Seq(prefix, name, UUID.randomUUID()).mkString("/")

  def writeSchema(schema: Schema, dest: Directory) {
    new OutputStreamWriter(dest.open("schema.avsc").write(), Charsets.UTF_8).
      append(schema.toString(true)).
      close()
  }

  def writeMeta(dest: Directory, written: Long = 0, transferredFrom: String = "") {
    val metaOut = dest.open("meta.avsc").overwrite()
    val encoder = EncoderFactory.get().jsonEncoder(SeriesMetadata.SCHEMA$, metaOut)
    val metaWriter: DatumWriter[SeriesMetadata] = new SpecificDatumWriter(SeriesMetadata.SCHEMA$)
    val meta = new SeriesMetadata()
    meta.entries = written
    meta.transferredFrom = transferredFrom;
    metaWriter.write(meta, encoder)
    encoder.flush()
    metaOut.close()
  }
}
