package ionic.server

import java.io.ByteArrayInputStream

import org.apache.avro.io.DecoderFactory

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.handler.codec.frame.CorruptedFrameException
import org.jboss.netty.handler.codec.frame.FrameDecoder

class AvroIntFrameDecoder extends FrameDecoder {

  override def decode(ctx: ChannelHandlerContext, channel: Channel, buffer: ChannelBuffer): Object = {
    buffer.markReaderIndex();
    val buf = new Array[Byte](5)
    for (i <- (0 until 5)) {
      if (!buffer.readable()) {
        buffer.resetReaderIndex()
        return null
      }

      buf(i) = buffer.readByte()
      if (buf(i) >= 0) {
        val in = new ByteArrayInputStream(buf)
        val length = DecoderFactory.get().directBinaryDecoder(in, null).readInt()
        if (length < 0) {
          throw new CorruptedFrameException("negative length: " + length);
        }

        if (buffer.readableBytes() < length) {
          buffer.resetReaderIndex();
          return null;
        } else {
          return buffer.readBytes(length);
        }
      }
    }

    // Couldn't find the byte whose MSB is off.
    throw new CorruptedFrameException("length wider than 32-bit");
  }
}
