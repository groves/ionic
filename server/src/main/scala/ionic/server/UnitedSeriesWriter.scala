package ionic.server

import java.io.RandomAccessFile
import java.util.UUID

import ionic.store.series.SeriesWriter
import ionic.store.series.UnitedSeriesReader

import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream

import com.threerings.fisy.impl.local.LocalDirectory

class UnitedSeriesWriter(schema: Schema, base: LocalDirectory) {

  private val dest =
    base.navigate(UnitedSeriesReader.dir(schema.getFullName()) + "/" + UUID.randomUUID().toString())

  SeriesWriter.writeSchema(schema, dest)

  private val series = new RandomAccessFile(dest.open("series").file(), "rwd").getChannel()

  private val decoderFactory = DecoderFactory.get()

  private var written = 0
  private var closed = false

  def write(buf: ChannelBuffer) {
    require(!closed)
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
    SeriesWriter.writeMeta(written, dest)
  }
}
