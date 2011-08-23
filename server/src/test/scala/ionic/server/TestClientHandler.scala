package ionic.test

import java.util.concurrent.CountDownLatch

import scala.collection.JavaConversions._

import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import org.jboss.netty.buffer.ChannelBufferOutputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class TestClientHandler(latch: CountDownLatch) extends SimpleChannelUpstreamHandler {
  override def channelConnected(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    val chan = e.getChannel()
    val buf = ChannelBuffers.dynamicBuffer(512)
    var enc = EncoderFactory.get.directBinaryEncoder(new ChannelBufferOutputStream(buf), null)
    enc.writeInt(0)
    enc.writeString(Event.SCHEMA$.toString())
    new SpecificDatumWriter(Event.SCHEMA$).write(new Event(), enc)
    chan.write(buf)
    latch.countDown()
  }
}
