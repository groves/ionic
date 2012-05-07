package ionic.net

import org.apache.avro.io.BinaryData

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder

class AvroIntLengthFieldPrepender extends OneToOneEncoder {
  override def encode(ctx: ChannelHandlerContext, channel: Channel, msg: Object): Object = {
    val body = msg match {
      case buf: ChannelBuffer => buf
      case _ => return msg
    }
    val length = body.readableBytes()
    val bytes = new Array[Byte](5)
    val encodedSize = BinaryData.encodeInt(length, bytes, 0)
    val header: ChannelBuffer =
      channel.getConfig().getBufferFactory().getBuffer(body.order(), encodedSize)
    header.writeBytes(bytes, 0, encodedSize)
    return ChannelBuffers.wrappedBuffer(header, body)
  }
}
