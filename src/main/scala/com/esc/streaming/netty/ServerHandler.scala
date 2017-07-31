package com.esc.streaming.netty

import java.nio.charset.Charset

import com.esc.streaming.core.{GlobalValue, RunSparkStreaming, Tasks}
import com.esc.streaming.message.{Attribute, ParseRegulation}
import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.group.DefaultChannelGroup
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.util.ReferenceCountUtil
import io.netty.util.concurrent.GlobalEventExecutor

/**
  * Created by pc-admin on 2017/6/26.
  */
class ServerHandler extends ChannelInboundHandlerAdapter{
  private val channelGroup = new DefaultChannelGroup("ChannelGroup",GlobalEventExecutor.INSTANCE)
  override def channelActive(ctx: ChannelHandlerContext): Unit = {
    println("channelActive invoked")
  }

  /**
    * 接受客户端发送来的消息
    */
  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    val channel = ctx.channel()
    println("receive connection :"+channel.remoteAddress())
    val byteBuf = msg.asInstanceOf[ByteBuf]
//    val bytes = new Array[Byte](byteBuf.readableBytes())
//    byteBuf.readBytes(bytes)
//    val message = new String(bytes, "UTF-8")
    val message = byteBuf.toString(Charset.forName("UTF-8"))
    println(message)
    ReferenceCountUtil.release(msg)
    if(message.equals("client")){
      channelGroup.add(channel)
    }
    if(message.contains("update")){
      val newName = message.split(":")(1)
      GlobalValue.regs = List(ParseRegulation("esc","es","(?<=name=)[a-zA-Z]+|(?<=age=)[0-9]+",10l,"select * from es where name='"+newName+"'",List(Attribute("name","string",0),Attribute("age","int",1))))
//      byteBuf.release()
      channelGroup.writeAndFlush("update finish!")
    }
//    GlobalValue.regs="fred"
//    GlobalValue.bc.unpersist()

//    val pass = GlobalValue.bc
//    GlobalValue.bc = GlobalValue.sc.broadcast("fred")
//    pass.unpersist()

//    val rss = GlobalValue.rss
//    val ssc  =rss.ssc
//    ssc.stop(false,true)
//    GlobalValue.rss = new RunSparkStreaming(GlobalValue.sc,GlobalValue.values,"fred")
//    new Thread(GlobalValue.rss).start()


    val back = "good boy!"
//    val backBytes = back.getBytes("UTF-8")
//    val buf = Unpooled.buffer()
//    buf.writeBytes(backBytes)
//    ctx.writeAndFlush(buf)
    val resp = Unpooled.copiedBuffer(back.getBytes("UTF-8"))
//    println(msg)
    ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE)
//    channel.close()
  }

  /**
    * 将消息对列中的数据写入到SocketChanne并发送给对方
    */
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    println("channekReadComplete invoked")
    ctx.flush()
  }
}
