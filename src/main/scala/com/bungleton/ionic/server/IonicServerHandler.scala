package com.bungleton.ionic.server

import org.apache.avro.specific.SpecificDatumReader
import com.bungleton.ionic.store.AddSchemas
import org.apache.avro.io.DecoderFactory
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class IonicServerHandler extends SimpleChannelUpstreamHandler {
  override def messageReceived (ctx :ChannelHandlerContext, e :MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    val added = new SpecificDatumReader(classOf[AddSchemas]).read(null, decoder)
    println(added.schemas)
  }
}
