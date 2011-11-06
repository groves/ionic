package ionic.client

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._
import scala.collection.mutable.ConcurrentMap

import com.codahale.logula.Logging

import org.apache.avro.Schema
import org.apache.avro.io.DecoderFactory

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

class SchemaMapper extends SimpleChannelUpstreamHandler with Logging {
  private val schemas: ConcurrentMap[Schema, Long] = new ConcurrentHashMap[Schema, Long]
  def apply(schema: Schema): Option[Long] = schemas.get(schema)
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = DecoderFactory.get().directBinaryDecoder(in, null)
    val schema = Schema.parse(decoder.readString(null).toString())
    val idx = decoder.readLong()
    log.info("Mapping '%s' to '%s'", schema, idx)
    schemas(schema) = idx
  }
}
