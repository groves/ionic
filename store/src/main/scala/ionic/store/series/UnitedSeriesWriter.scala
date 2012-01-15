package ionic.store.series

import java.io.RandomAccessFile
import java.nio.ByteBuffer

import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory

import react.Value
import react.ValueView

import com.threerings.fisy.Directory
import com.threerings.fisy.impl.local.LocalDirectory

class UnitedSeriesWriter(val schema: Schema, base: LocalDirectory) {

  val dest = base.navigate(SeriesWriter.genDir(Series.unitedPrefix, schema.getFullName()))

  SeriesWriter.writeSchema(schema, dest)

  // Opening with rwd forces data syncs on every write, but allows lazy metadata syncs
  private val series = new RandomAccessFile(dest.open("series").file(), "rwd").getChannel()

  private var _written = 0L

  def written = _written

  private val mutableClosed = new Value[Boolean](false)
  val closed: ValueView[Boolean] = mutableClosed

  def write(buf: ByteBuffer) {
    require(!mutableClosed.get)
    // TODO - validate data matches schema
    series.write(buf)
    _written += 1
  }

  def close() {
    if (mutableClosed.get) return
    series.close()
    SeriesWriter.writeMeta(dest, written)
    mutableClosed.update(true)
  }
}
