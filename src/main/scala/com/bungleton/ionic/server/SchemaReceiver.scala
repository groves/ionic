package com.bungleton.ionic.server

import com.threerings.fisy.Directory
import org.apache.avro.Schema
import scala.collection.immutable.IndexedSeq
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.io.DecoderFactory
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class SchemaReceiver(entries: Directory) extends SimpleChannelUpstreamHandler {
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = DecoderFactory.get().binaryDecoder(in, null)
    var schemas = Array.fill(decoder.readArrayStart().toInt) {
      Schema.parse(decoder.readString(null).toString())
    }
    ctx.getPipeline().replace(this, "entryReceiver", new EntryReceiver(schemas, entries))
  }
}
