package ionic.server

import scala.collection.mutable.Buffer
import scala.collection.mutable.Map

import com.codahale.logula.Logging

import ionic.store.Store
import ionic.store.series.SeriesWriter
import ionic.store.series.UnitedSeriesWriter

import org.apache.avro.Schema
import org.apache.avro.io.BinaryDecoder
import org.apache.avro.io.BinaryEncoder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.util.Utf8

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.buffer.ChannelBufferOutputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class SeriesReceiver(store: Store)
  extends SimpleChannelUpstreamHandler with Logging {
  private val decoderFactory = DecoderFactory.get()
  private var decoder: BinaryDecoder = null
  private val encoderFactory = EncoderFactory.get()
  private var encoder: BinaryEncoder = null
  private val schemas: Map[Schema, Int] = Map()
  private val writers: Buffer[UnitedSeriesWriter] = Buffer()

  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val inbuf = e.getMessage().asInstanceOf[ChannelBuffer]
    val in = new ChannelBufferInputStream(inbuf)
    decoder = decoderFactory.directBinaryDecoder(in, decoder)
    var postwrite: Function0[Unit] = () => {}
    val writer = decoder.readLong() match {
      case 0 => {
        val schemaUtf8: Utf8 = decoder.readString(null)
        val schema = Schema.parse(schemaUtf8.toString())

        schemas.get(schema) match {
          case Some(idx) => writers(idx)
          case None => {
            writers += store.writer(schema)
            schemas.put(schema, writers.size - 1)
            // Don't send the success until the write succeeds
            postwrite = () => {
              val outbuf = ChannelBuffers.dynamicBuffer(512)
              val outstream = new ChannelBufferOutputStream(outbuf)
              encoder = encoderFactory.directBinaryEncoder(outstream, encoder)
              encoder.writeBoolean(false)
              encoder.writeString(schemaUtf8)
              encoder.writeInt(writers.size - 1)
              ctx.getChannel().write(outbuf)
            }
            writers.last
          }
        }
      }
      case 1 => writers(decoder.readInt())
    }
    writer.write(inbuf.toByteBuffer())
    postwrite()
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    writers.foreach(_.close())
  }
}
