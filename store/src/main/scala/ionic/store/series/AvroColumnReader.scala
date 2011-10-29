package ionic.store.series

import org.apache.avro.io.DecoderFactory
import com.threerings.fisy.Directory
import org.apache.avro.Schema

abstract class AvroColumnReader(source: Directory, field: Schema.Field) extends ColumnReader {
  private val in = source.open(field.name).read()
  val decoder = DecoderFactory.get().binaryDecoder(in, null)

  def close() { in.close() }
}
