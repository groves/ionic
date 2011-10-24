package ionic.server

import java.io.OutputStreamWriter
import java.io.RandomAccessFile

import com.google.common.base.Charsets

import ionic.store.series.SeriesMetadata

import org.apache.avro.Schema
import org.apache.avro.io.DatumWriter
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream

import com.threerings.fisy.impl.local.LocalDirectory

class ImmediateSeriesWriter(schema: Schema, dest: LocalDirectory) {

  new OutputStreamWriter(dest.open("schema.avsc").write(), Charsets.UTF_8).
    append(schema.toString(true)).
    close()

  private val series = new RandomAccessFile(dest.open("series").file(), "rwd").getChannel()

  private val decoderFactory = DecoderFactory.get()

  private var written = 0
  private var closed = false

  def write(buf: ChannelBuffer) {
    /*buf.markReaderIndex()
    val decoder = decoderFactory.validatingDecoder(schema,
      decoderFactory.binaryDecoder(new ChannelBufferInputStream(buf), null))
    decoder.skipTopSymbol()
    buf.resetReaderIndex()*/
    series.write(buf.toByteBuffer())
    written += 1
  }

  def close() {
    if (closed) return
    closed = true
    series.close()
    val metaOut = dest.open("meta.avsc").write()
    val encoder = EncoderFactory.get().jsonEncoder(SeriesMetadata.SCHEMA$, metaOut)
    val metaWriter: DatumWriter[SeriesMetadata] = new SpecificDatumWriter(SeriesMetadata.SCHEMA$)
    val meta = new SeriesMetadata()
    meta.entries = written
    metaWriter.write(meta, encoder)
    encoder.flush()
    metaOut.close()
  }
}
