package com.bungleton.ionic.test

import com.bungleton.ionic.store.AddSchemas
import org.jboss.netty.buffer.ChannelBufferOutputStream
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.ChannelStateEvent
import java.util.concurrent.CountDownLatch
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import scala.collection.JavaConversions._


class TestClientHandler() extends SimpleChannelUpstreamHandler {
  override def channelConnected(ctx :ChannelHandlerContext, e :ChannelStateEvent) {
    val chan = e.getChannel()
    val buf = ChannelBuffers.buffer(4096)
    val enc = EncoderFactory.get.directBinaryEncoder(new ChannelBufferOutputStream(buf), null)
    val toAdd = new AddSchemas()
    toAdd.schemas = List(Event.SCHEMA$.toString())
    new SpecificDatumWriter(classOf[AddSchemas]).write(toAdd, enc)
    chan.write(buf)
  }

}
