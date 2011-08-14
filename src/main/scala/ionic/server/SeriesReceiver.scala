package ionic.server

import java.util.UUID

import ionic.store.series.SeriesWriter

import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

import com.threerings.fisy.Directory

class SeriesReceiver(schemas: IndexedSeq[Schema], entries: Directory)
  extends SimpleChannelUpstreamHandler {
  private val factory = DecoderFactory.get()
  private val writers = schemas.map(s => {
    val subdir = s.getFullName() + "/" + UUID.randomUUID().toString()
    new SeriesWriter(s, entries.navigate(subdir))
  })
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = factory.directBinaryDecoder(in, null)
    val idx = decoder.readInt()
    writers(idx).write(decoder)
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    writers.foreach(_.close())
  }
}
