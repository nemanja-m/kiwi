package kiwi.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class KiwiServer {
    private static final Logger logger = LoggerFactory.getLogger(KiwiServer.class);

    private static final int BUFFER_SIZE = 1024 * 1024;

    private final String host;
    private final int port;

    public KiwiServer(int port) {
        this.host = "0.0.0.0";
        this.port = port;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);

        try {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel socketChannel) {
                            socketChannel.pipeline().addLast(new KiwiServerHandler());
                        }
                    })
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_RCVBUF, BUFFER_SIZE)
                    .childOption(ChannelOption.SO_SNDBUF, BUFFER_SIZE)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            ChannelFuture future = bootstrap.bind(host, port).sync();

            logger.info("Listening at {}:{}", host, port);

            // Wait until the server socket is closed.
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    static class KiwiServerHandler extends ChannelInboundHandlerAdapter {

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            ByteBuf in = (ByteBuf) msg;
            try {
                String command = in.toString(StandardCharsets.UTF_8).trim();
                logger.info("Received command: {}", command);
                if ("PING".equalsIgnoreCase(command)) {
                    String pong = "PONG\n";
                    ByteBuf response = ctx.alloc().buffer(pong.getBytes(StandardCharsets.UTF_8).length);
                    response.writeCharSequence(pong, StandardCharsets.UTF_8);
                    ctx.writeAndFlush(response);
                }
            } finally {
                ReferenceCountUtil.release(msg);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            logger.error("Exception caught", cause);
            ctx.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        KiwiServer server = new KiwiServer(6379);
        server.start();
    }
}
