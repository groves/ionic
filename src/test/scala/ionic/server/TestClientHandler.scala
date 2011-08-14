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
    val schemaBuf = ChannelBuffers.dynamicBuffer(512)
    var enc = EncoderFactory.get.directBinaryEncoder(new ChannelBufferOutputStream(schemaBuf), null)
    enc.writeArrayStart()
    enc.setItemCount(1)
    enc.startItem()
    enc.writeString(Event.SCHEMA$.toString())
    enc.writeArrayEnd()
    chan.write(schemaBuf)

    val entryBuf = ChannelBuffers.dynamicBuffer(512)
    enc = EncoderFactory.get.directBinaryEncoder(new ChannelBufferOutputStream(entryBuf), enc)
    enc.writeInt(0)
    enc.writeArrayStart()
    enc.setItemCount(1)
    enc.startItem()
    val ev = new Event()
    new SpecificDatumWriter(Event.SCHEMA$).write(new Event(), enc)
    chan.write(entryBuf)
    latch.countDown()
  }
}
