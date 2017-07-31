package com.esc.streaming.netty

import io.netty.bootstrap.Bootstrap
import io.netty.channel.ChannelInitializer
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.serialization.{ClassResolvers, ObjectDecoder}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by pc-admin on 2017/6/26.
  */
class NettyClient(sc:SparkContext,var bc:Broadcast[String]) extends Runnable{
  def connect(host: String, port: Int): Unit = {
    //创建客户端NIO线程组
    val eventGroup = new NioEventLoopGroup
    //创建客户端辅助启动类
    val bootstrap = new Bootstrap
    try {
      //将NIO线程组传入到Bootstrap
      bootstrap.group(eventGroup)
        //创建NioSocketChannel
        .channel(classOf[NioSocketChannel])
        //绑定I/O事件处理类
        .handler(new ChannelInitializer[SocketChannel] {
        override def initChannel(ch: SocketChannel): Unit = {
          ch.pipeline()
            .addLast("decoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
            .addLast(
                new ObjectDecoder(ClassResolvers.cacheDisabled(getClass.getClassLoader)),
            new ClientHandler(sc,bc)
          )
        }
      })
      //发起异步连接操作
      val channelFuture = bootstrap.connect(host, port).sync()

      //等待服务关闭
      channelFuture.channel().closeFuture().sync()
    } finally {
      //优雅的退出，释放线程池资源
      eventGroup.shutdownGracefully()
    }
  }

  override def run(): Unit = {
    connect("192.168.254.128",56789)
  }
}
