package ionic.server

import java.io.File
import java.net.InetSocketAddress
import java.net.SocketAddress
import java.util.concurrent.Executors

import ionic.net.AvroIntFrameDecoder
import ionic.net.AvroIntLengthFieldPrepender

import org.jboss.netty.bootstrap.ServerBootstrap
import org.jboss.netty.channel.Channel
import org.jboss.netty.channel.ChannelFuture
import org.jboss.netty.channel.ChannelPipelineFactory
import org.jboss.netty.channel.Channels
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory

import com.threerings.fisy.Directory
import com.threerings.fisy.impl.local.LocalDirectory

/** Binds a server with the given bootstrap, which must have a localAddress set on it. */
class IonicServer(boot: ServerBootstrap, entries: Directory) {
  boot.setPipelineFactory(new ChannelPipelineFactory() {
    override def getPipeline() = {
      // TODO - add an executor since we're doing disk IO
      Channels.pipeline(new AvroIntLengthFieldPrepender(), new AvroIntFrameDecoder(),
        new SchemaReceiver(entries))
    }
  })
  val channel: Channel = boot.bind()
  val address: SocketAddress = channel.getLocalAddress

  def close(): ChannelFuture = { channel.close() }
}

object IonicServer {
  val port = 10713

  def createTempDirectory(): Directory = {
    val dir = new File(System.getProperty("java.io.tmpdir"), "ionic-entries")
    dir.mkdir()
    new LocalDirectory(dir)
  }

  def main(args: Array[String]) {
    val boot = new ServerBootstrap(new NioServerSocketChannelFactory(
      Executors.newCachedThreadPool(), Executors.newCachedThreadPool()))
    boot.setOption("localAddress", new InetSocketAddress(port))
    new IonicServer(boot, createTempDirectory())
  }
}
