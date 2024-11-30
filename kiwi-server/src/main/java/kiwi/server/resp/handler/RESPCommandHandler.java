package kiwi.server.resp.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import kiwi.core.common.Bytes;
import kiwi.core.storage.KeyValueStore;
import kiwi.server.resp.command.RESPCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

public class RESPCommandHandler extends SimpleChannelInboundHandler<RESPCommand> {
    private static final Logger logger = LoggerFactory.getLogger(RESPCommandHandler.class);

    private final KeyValueStore<Bytes, Bytes> db;

    public RESPCommandHandler(KeyValueStore<Bytes, Bytes> db) {
        this.db = db;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, RESPCommand command) {
        switch (command.commandType()) {
            case PING -> handlePing(ctx, command);
            case INFO -> handleInfo(ctx, command);
            case SELECT -> handleSelect(ctx, command);
            case COMMAND -> handleCommand(ctx, command);
            case CONFIG -> handleConfig(ctx, command);
            case SET -> handleSet(ctx, command);
            case GET -> handleGet(ctx, command);
            case DEL -> handleDelete(ctx, command);
            case EXISTS -> handleExists(ctx, command);
            case DBSIZE -> handleSize(ctx, command);
            case FLUSHDB -> handleFlush(ctx, command);
            case UNKNOWN -> handleUnknown(ctx, command);
        }
    }

    private void handlePing(ChannelHandlerContext ctx, RESPCommand command) {
        if (command.arguments().size() == 1) {
            byte[] echo = command.arguments().getFirst().getBytes(StandardCharsets.UTF_8);
            ctx.writeAndFlush(echo);
        } else {
            ctx.writeAndFlush("PONG");
        }
    }

    private void handleInfo(ChannelHandlerContext ctx, RESPCommand ignoredCommand) {
        String info = "# Server\r\nkiwi_version:0.1.0\r\nkiwi_mode:standalone";
        ctx.writeAndFlush(info.getBytes(StandardCharsets.UTF_8));
    }

    private void handleSelect(ChannelHandlerContext ctx, RESPCommand ignoredCommand) {
        // There is only one database in Kiwi, so we always return OK.
        ctx.writeAndFlush("OK");
    }

    private void handleCommand(ChannelHandlerContext ctx, RESPCommand ignoredCommand) {
        // COMMAND DOCS is not supported yet, so we always return empty array.
        ctx.writeAndFlush(new String[0]);
    }

    private void handleConfig(ChannelHandlerContext ctx, RESPCommand command) {
        if (command.arguments().size() != 2) {
            ctx.writeAndFlush(new Throwable("CONFIG requires 2 arguments"));
            return;
        }
        switch (command.arguments().getLast().toLowerCase()) {
            case "save" -> ctx.writeAndFlush(new String[]{"save", ""});
            case "appendonly" -> ctx.writeAndFlush(new String[]{"appendonly", "yes"});
        }
        ctx.writeAndFlush(new String[]{"save", ""});
    }

    private void handleSet(ChannelHandlerContext ctx, RESPCommand command) {
        if (command.arguments().size() != 2) {
            ctx.writeAndFlush(new Throwable("SET requires 2 arguments"));
            return;
        }
        Bytes key = Bytes.wrap(command.arguments().getFirst().getBytes(StandardCharsets.UTF_8));
        Bytes value = Bytes.wrap(command.arguments().getLast().getBytes(StandardCharsets.UTF_8));
        db.put(key, value);
        ctx.writeAndFlush("OK");
    }

    private void handleGet(ChannelHandlerContext ctx, RESPCommand command) {
        if (command.arguments().size() != 1) {
            ctx.writeAndFlush(new Throwable("GET requires 1 argument"));
            return;
        }
        Bytes key = Bytes.wrap(command.arguments().getFirst().getBytes(StandardCharsets.UTF_8));
        String value = db.get(key).map(Bytes::toString).orElse(null);
        ctx.writeAndFlush(value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8));
    }

    private void handleDelete(ChannelHandlerContext ctx, RESPCommand command) {
        if (command.arguments().size() != 1) {
            ctx.writeAndFlush(new Throwable("DEL requires 1 argument"));
            return;
        }
        Bytes key = Bytes.wrap(command.arguments().getFirst().getBytes(StandardCharsets.UTF_8));
        db.delete(key);
        ctx.writeAndFlush("OK");
    }

    private void handleExists(ChannelHandlerContext ctx, RESPCommand command) {
        if (command.arguments().size() != 1) {
            ctx.writeAndFlush(new Throwable("EXISTS requires 1 argument"));
            return;
        }
        Bytes key = Bytes.wrap(command.arguments().getFirst().getBytes(StandardCharsets.UTF_8));
        boolean exists = db.contains(key);
        ctx.writeAndFlush(exists ? 1 : 0);
    }

    private void handleSize(ChannelHandlerContext ctx, RESPCommand ignoredCommand) {
        long size = db.size();
        ctx.writeAndFlush(size);
    }

    private void handleFlush(ChannelHandlerContext ctx, RESPCommand ignoredCommand) {
        db.purge();
        ctx.writeAndFlush("OK");
    }

    private void handleUnknown(ChannelHandlerContext ctx, RESPCommand command) {
        ctx.writeAndFlush(new Throwable("unknown command: " + command.commandType()));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        logger.error("Error while handling RESP command", cause);
        ctx.close();
    }
}
