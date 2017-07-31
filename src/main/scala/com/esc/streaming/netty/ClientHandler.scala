package com.esc.streaming.netty

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{ChannelHandlerContext, ChannelInboundHandlerAdapter}
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

/**
  * Created by pc-admin on 2017/6/26.
  */
class ClientHandler(sc:SparkContext,var bc:Broadcast[String]) extends ChannelInboundHandlerAdapter{
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("channelActive")
    val content = "client"
    ctx.writeAndFlush(Unpooled.copiedBuffer(content.getBytes("UTF-8")))
  }

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("channelRead")
    val byteBuf = msg.asInstanceOf[ByteBuf]
    val bytes = new Array[Byte](byteBuf.readableBytes())
    byteBuf.readBytes(bytes)
    val message = new String(bytes, "UTF-8")
    println(message)
    bc = sc.broadcast(message)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    println("channeReadComplete")
    ctx.flush()
  }
  //发送异常时关闭
  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    println("exceptionCaught")
    ctx.close()
  }
}
