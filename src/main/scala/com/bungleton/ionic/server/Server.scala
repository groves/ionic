package com.bungleton.ionic.server

import java.net.InetSocketAddress
import org.apache.avro.ipc.SocketServer
import ionic.Store

object Server {
  def main (args :Array[String]) {
    val server = new SocketServer(new StoreResponder(), new InetSocketAddress(10713))
    server.start()
    server.join()
  }
}
