package ionic.server

import java.util.UUID

import scala.collection.mutable.Buffer
import scala.collection.mutable.Map

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

class SeriesReceiver(entries: Directory)
  extends SimpleChannelUpstreamHandler {
  private val factory = DecoderFactory.get()
  private val schemas: Map[Schema, Int] = Map()
  private val writers: Buffer[SeriesWriter] = Buffer()
  private var written: Int = 0
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = factory.directBinaryDecoder(in, null)
    val schema = Schema.parse(decoder.readString(null).toString())

    val writer = schemas.get(schema) match {
      case Some(idx) => writers(idx)
      case None => {
        val subdir = schema.getFullName() + "/" + UUID.randomUUID().toString()
        println(entries.navigate(subdir))
        writers += new SeriesWriter(schema, entries.navigate(subdir))
        schemas.put(schema, writers.size - 1)
        writers.last
      }
    }
    writer.write(decoder)
    written += 1
    if (written % 10000 == 0) {
      println("Wrote " + written)
    }
  }

  override def channelClosed(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
    writers.foreach(_.close())
  }
}
