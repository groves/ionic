package ionic.server

import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.concurrent.Executors

import com.codahale.logula.Logging

import ionic.net.AvroIntFrameDecoder
import ionic.net.AvroIntLengthFieldPrepender
import ionic.store.Store

import org.apache.avro.io.EncoderFactory
import org.apache.log4j.Level

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.buffer.ChannelBufferOutputStream
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelHandlerContext
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.ChannelStateEvent
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.ExceptionEvent
import org.jboss.netty.channel.SimpleChannelUpstreamHandler
import org.jboss.netty.channel.group.DefaultChannelGroup
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory

import sun.misc.Signal
import sun.misc.SignalHandler

import com.threerings.fisy.Directory
import com.threerings.fisy.OperationException
import com.threerings.fisy.Paths
import com.threerings.fisy.impl.local.LocalDirectory

/** Binds a server with the given bootstrap, which must have a localAddress set on it. */
class IonicServer(boot: ServerBootstrap, base: LocalDirectory) extends Logging {
  val allChannels = new DefaultChannelGroup()
  val store = new Store(base)
  val tracker = new SimpleChannelUpstreamHandler() {
    override def channelOpen(ctx: ChannelHandlerContext, e: ChannelStateEvent) {
      allChannels.add(e.getChannel())
    }

    override def exceptionCaught(ctx: ChannelHandlerContext, e: ExceptionEvent) {
      e.getCause match {
        case oe: OperationException => {
          log.warn(oe, "Got io exception. Telling the client to buzz off.")
        }
        case e: Throwable => {
          log.warn(e, "Got unknown exception. Telling the client to buzz off.")
        }
      }
      val outbuf = ChannelBuffers.dynamicBuffer(512)
      val outstream = new ChannelBufferOutputStream(outbuf)
      val encoder = EncoderFactory.get().directBinaryEncoder(outstream, null)
      encoder.writeBoolean(true)
      encoder.writeString(e.getCause.getMessage)
      ctx.getChannel().write(outbuf)
    }
  }
  boot.setPipelineFactory(new ChannelPipelineFactory() {
    override def getPipeline() = {
      // TODO - add an executor since we're doing disk IO
      Channels.pipeline(new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(),
        new SeriesReceiver(store), tracker)
    }
  })
  log.info("Binding to %s", boot.getOption("localAddress"))
  val channel: Channel = boot.bind()
  val address: SocketAddress = channel.getLocalAddress

  def shutdown() {
    // TODO - close store
    allChannels.close().awaitUninterruptibly()
    boot.releaseExternalResources()
  }
}

object IonicServer extends Logging {
  val port = 10713

  def main(args: Array[String]) {
    Logging.configure { log => log.level = Level.INFO }
    val boot = new ServerBootstrap(new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
    boot.setOption("localAddress", new InetSocketAddress(port))
    val server = new IonicServer(boot, Paths.makeTempFs())
    Signal.handle(new Signal("INT"), new SignalHandler() {
      override def handle(sig: Signal) {
        log.info("Shutting down due to interrupt signal")
        server.shutdown()
        log.info("Shut down")
      }
    })
  }
}
