package ionic.client

import java.nio.channels.ClosedChannelException
import java.util.concurrent.BlockingQueue
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

import scala.actors.Actor
import scala.actors.TIMEOUT
import scala.collection.JavaConversions._
import scala.collection.mutable.ConcurrentMap

import com.codahale.logula.Logging

import ionic.net.AvroIntFrameDecoder
import ionic.net.AvroIntLengthFieldPrepender

import org.apache.avro.Schema
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.specific.SpecificDatumWriter

import org.jboss.netty.bootstrap.ClientBootstrap
import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.buffer.ChannelBufferInputStream
import org.jboss.netty.buffer.ChannelBufferOutputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelFutureListener
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

case class QueueInserted()
case class CtxReady(ctx :NettyMsgContext)
case class CtxFailed(ctx: NettyMsgContext, rec: IndexedRecord, cause :Throwable)
case class ChannelClosed(chan: Channel)
case class ServerError(error: String)
case class Shutdown(latch: CountDownLatch)

class RecordSender(queue: BlockingQueue[IndexedRecord], boot: ClientBootstrap) extends Actor with Logging {
  import ionic.client.ChannelFutureListenerImplicit._

  var ctxs :List[NettyMsgContext] = List()
  val initialLength = 10
  var chan: Channel = null
  var mapper: SchemaMapper = null
  start

  def act () {
    mapper = new SchemaMapper(this)
    // Make the initial connection as we're ready to get the channel
    boot.setPipelineFactory(Channels.pipelineFactory(Channels.pipeline(
      new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(), mapper)))
    boot.connect().addListener((future: ChannelFuture) => {
     if (future.isSuccess()) this ! future.getChannel
     // TODO - attempt reconnect after delay
     else if (future.getCause() != null) log.warn(future.getCause(), "Failed to connect")
     else log.warn("Ionic connection cancelled?")
    })
    withConn()
  }

  def sendFromQueue(ctx :NettyMsgContext): Boolean = queue.poll() match {
    case null => false
    case rec => {
      ctx.write(rec)
      true
    }
  }

  def reconnect() = {
    chan = null
    ctxs = List()
    act()
  }

  def withConn() :Unit = react {
    case newChan :Channel => {
      assert(chan == null, "Got a new channel while we still have a channel?")
      chan = newChan
      chan.getCloseFuture.addListener((_ :ChannelFuture) => { this ! ChannelClosed(newChan) })
      (0 until initialLength).foreach((_) => this ! CtxReady(new NettyMsgContext(chan, this, mapper)))
      withConn()
    }
    case QueueInserted => {
      if (!ctxs.isEmpty && sendFromQueue(ctxs.head)) ctxs = ctxs.tail
      //TODO - spool if over 1024 in queue
      withConn()
    }
    case CtxReady(ctx) => {
      if (!sendFromQueue(ctx)) ctxs = ctx :: ctxs
      withConn()
    }
    case CtxFailed(ctx, rec, cause) => {
      queue.put(rec)
      this ! QueueInserted
      if (ctx.chan == chan) reconnect()
      // If the channel has already changed, we're reconnecting
    }
    case ChannelClosed(closed) => {
      assert(closed == chan, "Unknown channel closed?")
      log.warn("Lost connection; reconnecting")
      reconnect()
    }
    case ServerError(error) => {
      chan = null
      ctxs = List()
      // TODO - have server send if retry should be attempted
      log.warn("Throwing up hands due to server error: %s", error)
      spool()
    }
    case Shutdown(latch) => {
      if (needsDrain) drain(List(latch))
      else close(List(latch))
    }
    case msg => {
      log.warn("RecordSender received unknown message: %s", msg)
      withConn()
    }
  }

  def needsDrain = ctxs.length < initialLength && chan != null

  def spool () {}// TODO - in error state just write to disk

  def drained(ctx :NettyMsgContext) :Boolean = {
    ctxs = ctx ::ctxs
    return !needsDrain
  }

  def drain(latches: List[CountDownLatch]): Unit = reactWithin(10000) {
    case QueueInserted => {
      log.warn("Got queue insertion after being asked to shutdown")
      drain(latches)
    }
    case CtxReady(ctx) => {
      if (!sendFromQueue(ctx) && drained(ctx)) close(latches)
      else drain(latches)
    }
    case CtxFailed(ctx, rec, cause) => {
      queue.put(rec)
      cause match {
        case cce :ClosedChannelException => {}
        case _ => log.warn(cause, "Got error from writing while draining")
      }
      if (drained(ctx)) close(latches)
      else drain(latches)
    }
    case ServerError(error) => {
      log.warn("Got error from server while draining: %s", error)
      close(latches)
    }
    case TIMEOUT => {
      log.warn("Didn't get a writer ready within 10 seconds, shutting down without finishing draining queue")
      close(latches)
    }
    case Shutdown(latch) => drain(latch :: latches)
    case msg => {
      log.warn("RecordSender received unknown message: %s", msg)
      drain(latches)
    }
  }

  def close(latches :List[CountDownLatch]): Unit = {
    if (chan != null) chan.close()
    // TODO - queue to disk
    latches.foreach(_.countDown())
  }

  def shutdown() {
    val latch = new CountDownLatch(1)
    this ! Shutdown(latch)
    latch.await(60, TimeUnit.SECONDS)
  }
}
class SchemaMapper(listener :Actor) extends SimpleChannelUpstreamHandler with Logging {
  private val schemas: ConcurrentMap[Schema, Long] = new ConcurrentHashMap[Schema, Long]
  def apply(schema: Schema): Option[Long] = schemas.get(schema)
  override def messageReceived(ctx: ChannelHandlerContext, e: MessageEvent) {
    val in = new ChannelBufferInputStream(e.getMessage().asInstanceOf[ChannelBuffer])
    val decoder = DecoderFactory.get().directBinaryDecoder(in, null)
    if (decoder.readBoolean()) listener ! ServerError(decoder.readString(null).toString)
    else {
      val schema = Schema.parse(decoder.readString(null).toString())
      val idx = decoder.readLong()
      log.info("Mapping '%s' to '%s'", schema, idx)
      schemas(schema) = idx
    }
  }

  override def exceptionCaught(ctx :ChannelHandlerContext, e :ExceptionEvent) {
    log.debug(e.getCause, "Thrown by conn; should be passed to the receiver by the ctx writer")
  }
}

class NettyMsgContext(val chan :Channel, writer: Actor, mapper: SchemaMapper) extends Logging {
  import ionic.client.ChannelFutureListenerImplicit._

  val buf = ChannelBuffers.dynamicBuffer(512)
  val enc = EncoderFactory.get.directBinaryEncoder(new ChannelBufferOutputStream(buf), null)
  var rec: IndexedRecord = null

  def write(record: IndexedRecord) {
    rec = record
    mapper(record.getSchema()) match {
      case Some(idx) => {
        enc.writeInt(1)
        enc.writeLong(idx)
      }
      case None => {
        enc.writeInt(0)
        enc.writeString(record.getSchema().toString())
      }
    }
    // TODO - cache writers per schema
    new SpecificDatumWriter(record.getSchema).write(record, enc)

    chan.write(buf).addListener((future :ChannelFuture) => {
      buf.clear()
      if (future.isSuccess()) writer ! CtxReady(this)
      else if (future.getCause() != null) writer ! CtxFailed(this, rec, future.getCause())
      else log.warn("Got something other than failure or success to a write? [cancelled=%s]",
          future.isCancelled)
    })
  }
}
