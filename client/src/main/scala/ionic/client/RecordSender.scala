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

import ionic.client.ChannelFutureListenerImplicit._
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
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.MessageEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler

case object QueueInserted
case class Connected(chan :Channel, mapper :SchemaMapper)
case class CtxReady(ctx :NettyMsgContext)
case class CtxFailed(ctx: NettyMsgContext, rec: IndexedRecord, cause :Throwable)
case class ChannelClosed(chan: Channel)
case class ServerError(error: String)
case class Shutdown(latch: CountDownLatch)

class RecordSender(queue: BlockingQueue[IndexedRecord], boot: ClientBootstrap) extends Actor with Logging {
  val connector :Connector = new Connector(this, boot)
  var ctxs :List[NettyMsgContext] = List()
  val initialLength = 10
  var chan: Channel = null
  start

  def sendFromQueue(ctx :NettyMsgContext): Boolean = queue.poll() match {
    case null => false
    case rec => {
      ctx.write(rec)
      true
    }
  }

  def reconnect(closed :Channel) :Unit = {
    if (chan == null) return // Already reconnecting
    else if (closed != chan) {
      log.warn("Another channel connected besides the closed one?")
      return
    }
    chan = null
    ctxs = List()
    connector ! 'connect
  }

  def act() :Unit = react {
    case Connected(newChan :Channel, mapper :SchemaMapper) => {
      assert(chan == null, "Got a new channel while we still have a channel?")
      chan = newChan
      chan.getCloseFuture.addListener((_ :ChannelFuture) => { this ! ChannelClosed(newChan) })
      (0 until initialLength).foreach((_) => this ! CtxReady(new NettyMsgContext(chan, this, mapper)))
      act()
    }
    case QueueInserted => {
      if (!ctxs.isEmpty && sendFromQueue(ctxs.head)) ctxs = ctxs.tail
      //TODO - spool if over 1024 in queue
      act()
    }
    case CtxReady(ctx) => {
      if (!sendFromQueue(ctx)) ctxs = ctx :: ctxs
      act()
    }
    case CtxFailed(ctx, rec, cause) => {
      queue.put(rec)
      this ! QueueInserted
      reconnect(ctx.chan)
      act()
    }
    case ChannelClosed(closed) => {
      reconnect(closed)
      act()
    }
    case ServerError(error) => {
      chan = null
      ctxs = List()
      // TODO - have server send if retry should be attempted
      log.warn("Throwing up hands due to server error: %s", error)
      spool()
    }
    case Shutdown(latch) => {
      connector ! 'shutdown
      if (needsDrain) drain(List(latch))
      else close(List(latch))
    }
    case msg => {
      log.warn("RecordSender received unknown message: %s", msg)
      act()
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
    exit()
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

case class ConnectingFailed(retryDelay :Long)
class Connector(user :Actor, boot :ClientBootstrap) extends Actor with Logging {

  var consecutiveFailures :Int = 0

  start

  def awaitOrders () :Unit = react {
    case 'connect => act()
    case 'shutdown => exit()
  }

  def awaitConnection () = react {
    case 'connected => awaitOrders()
    case ConnectingFailed(delay) => awaitRetry(delay)
    case 'shutdown => exit()
  }

  def awaitRetry (retryDelay :Long) = reactWithin(retryDelay) {
    case TIMEOUT => act()
    case 'shutdown => exit()
  }

  def act () :Unit = {
    val mapper = new SchemaMapper(user)
    // Make the initial connection as we're ready to get the channel
    boot.setPipelineFactory(Channels.pipelineFactory(Channels.pipeline(
      new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(), mapper)))
    boot.connect().addListener((future: ChannelFuture) => {
      if (future.isSuccess) {
        consecutiveFailures = 0
        user ! Connected(future.getChannel, mapper)
        this ! 'connected
      } else if (future.getCause() != null) {
        // Ramp from 1 second to 5 minutes between retries
        val delay :Long = math.min(300, 1 << consecutiveFailures) * 1000;
        consecutiveFailures += 1
        if (consecutiveFailures % 3 == 0) {
          log.warn(future.getCause(), "Failed to connect %s times. Waiting %d millis before retrying.", consecutiveFailures, delay)
        }
        this ! ConnectingFailed(delay)
      } else {
        log.warn("Connecting cancelled?")
        this ! 'shutdown
      }
    })
    awaitConnection()
  }

}
