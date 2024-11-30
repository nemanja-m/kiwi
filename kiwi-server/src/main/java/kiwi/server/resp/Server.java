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
import kiwi.core.common.Bytes;
import kiwi.core.storage.KeyValueStore;
import kiwi.core.storage.bitcask.BitcaskStore;
import kiwi.server.config.Options;
import kiwi.server.resp.codec.RESPDecoder;
import kiwi.server.resp.codec.RESPEncoder;
import kiwi.server.resp.config.ServerConfig;
import kiwi.server.resp.handler.RESPCommandHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Server {
    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final ServerConfig config;

    public Server() {
        this(Options.defaults.server);
    }

    public Server(ServerConfig config) {
        this.config = config;
    }

    public void start() throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);

        try (BitcaskStore db = BitcaskStore.open()) {
            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ServerInitializer(db))
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true);

            ChannelFuture future = bootstrap.bind(config.host, config.port).sync();

            logger.info("Listening at {}:{}", config.host, config.port);

            // Wait until the server socket is closed.
            future.channel().closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    static class ServerInitializer extends ChannelInitializer<SocketChannel> {
        private final KeyValueStore<Bytes, Bytes> db;

        public ServerInitializer(KeyValueStore<Bytes, Bytes> db) {
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

    public static void main(String[] args) throws InterruptedException {
        Server server = new Server();
        server.start();
    }
}
