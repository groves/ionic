package ionic.store.series

import java.io.OutputStreamWriter

import scala.collection.JavaConversions._

import com.google.common.base.Charsets

import org.apache.avro.Schema

import com.threerings.fisy.Directory

object SeriesWriter {
  def writeSchema(schema: Schema, dest: Directory) {
    new OutputStreamWriter(dest.open("schema.avsc").write(), Charsets.UTF_8).
      append(schema.toString(true)).
      close()
  }
}
