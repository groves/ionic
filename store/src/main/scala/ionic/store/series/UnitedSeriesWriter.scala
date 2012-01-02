package ionic.store.series

import react.ValueView
import react.Value
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.util.UUID

import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory

import com.threerings.fisy.impl.local.LocalDirectory

class UnitedSeriesWriter(schema: Schema, base: LocalDirectory) {

  private val dest =
    base.navigate(UnitedSeriesReader.dir(schema.getFullName()) + "/" + UUID.randomUUID().toString())

  SeriesWriter.writeSchema(schema, dest)

  // Opening with rwd forces data syncs on every write, but allows lazy metadata syncs
  private val series = new RandomAccessFile(dest.open("series").file(), "rwd").getChannel()

  private val decoderFactory = DecoderFactory.get()

  private var written = 0
  private val mutableClosed = new Value[Boolean](false)

  val closed: ValueView[Boolean] = mutableClosed

  def write(buf: ByteBuffer) {
    require(!mutableClosed.get)
    // TODO - validate data matches schema
    series.write(buf)
    written += 1
  }

  def close() {
    if (mutableClosed.get) return
    mutableClosed.update(true)
    series.close()
    SeriesWriter.writeMeta(written, dest)
  }
}
