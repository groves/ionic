package ionic.server

import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

import com.threerings.fisy.Directory

class SchemaReceiver(entries: Directory) extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    var schemas = Array.fill(decoder.readArrayStart().toInt) {
      Schema.parse(decoder.readString(null).toString())
    }
    ctx.getPipeline().replace(this, "entryReceiver", new SeriesReceiver(schemas, entries))
  }
}
