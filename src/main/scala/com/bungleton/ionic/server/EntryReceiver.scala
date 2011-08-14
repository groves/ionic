package com.bungleton.ionic.server

import org.jboss.netty.channel.ChannelEvent
import org.jboss.netty.channel.ChannelStateEvent
import java.util.UUID
import com.threerings.fisy.Directory
import com.threerings.fisy.Directory
import org.jboss.netty.buffer.ChannelBuffer
import com.bungleton.ionic.store.EntryWriter
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.apache.avro.io.DecoderFactory
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.ChannelHandlerContext
import org.apache.avro.Schema
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class EntryReceiver(schemas :IndexedSeq[Schema], entries :Directory)
    extends SimpleChannelUpstreamHandler {
  private val factory = DecoderFactory.get()
  private val writers = schemas.map(s => {
    val subdir = s.getFullName() + "/" + UUID.randomUUID().toString()
    new EntryWriter(s, entries.navigate(subdir))
  })
  override def messageReceived (ctx :ChannelHandlerContext, e :MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = factory.directBinaryDecoder(in, null)
    val idx = decoder.readInt()
    writers(idx).write(decoder)
  }

  override def channelClosed (ctx :ChannelHandlerContext, e :ChannelStateEvent) {
    writers.foreach(_.close())
  }
}
