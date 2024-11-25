package kiwi.server.resp;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import kiwi.core.storage.bitcask.BitcaskStore;
import kiwi.server.resp.codec.RESPDecoder;
import kiwi.server.resp.codec.RESPEncoder;
import kiwi.server.resp.handler.RESPCommandHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private static final int BUFFER_SIZE = 1024 * 1024;

    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);

        try (BitcaskStore db = BitcaskStore.open(Paths.get("local/db"))) {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ServerInitializer(db))
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_RCVBUF, BUFFER_SIZE)
                    .childOption(ChannelOption.SO_SNDBUF, BUFFER_SIZE)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            String host = "0.0.0.0";
            int port = 6379;

            ChannelFuture future = bootstrap.bind(host, port).sync();

            logger.info("Listening at {}:{}", host, port);

            // Wait until the server socket is closed.
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    static class ServerInitializer extends ChannelInitializer<SocketChannel> {
        private final BitcaskStore db;

        public ServerInitializer(BitcaskStore db) {
            this.db = db;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            // Inbound
            ch.pipeline().addLast("decoder", new RESPDecoder());

            // Outbound
            ch.pipeline().addLast("encoder", new RESPEncoder());
            ch.pipeline().addLast("command", new RESPCommandHandler(db));
        }
    }
}
