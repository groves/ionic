package ionic.server

import java.util.UUID

import scala.collection.mutable.Buffer
import scala.collection.mutable.Map

import ionic.store.series.SeriesWriter

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.buffer.ChannelBufferOutputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

import com.threerings.fisy.Directory

class SeriesReceiver(entries: Directory)
  extends SimpleChannelUpstreamHandler {
  private val decoderFactory = DecoderFactory.get()
  private var decoder: BinaryDecoder = null
  private val encoderFactory = EncoderFactory.get()
  private var encoder: BinaryEncoder = null
  private val schemas: Map[String, Int] = Map()
  private val writers: Buffer[SeriesWriter] = Buffer()

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    decoder = decoderFactory.directBinaryDecoder(in, decoder)
    val writer = decoder.readLong() match {
      case 0 => {
        val schema = Schema.parse(decoder.readString(null).toString())

        // TODO - check for redefinition
        schemas.get(schema.getFullName()) match {
          case Some(idx) => writers(idx)
          case None => {
            val subdir = schema.getFullName() + "/" + UUID.randomUUID().toString()
            writers += new SeriesWriter(schema, entries.navigate(subdir))
            schemas.put(schema.getFullName(), writers.size - 1)
            val outbuf = ChannelBuffers.dynamicBuffer(512)
            val outstream = new ChannelBufferOutputStream(outbuf)
            encoder = encoderFactory.directBinaryEncoder(outstream, encoder)
            encoder.writeString(schema.getFullName)
            encoder.writeInt(writers.size - 1)
            ctx.getChannel().write(outbuf)
            writers.last
          }
        }
      }
      case 1 => writers(decoder.readInt())
    }
    writer.write(decoder)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    writers.foreach(_.close())
  }
}
