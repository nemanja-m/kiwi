package kiwi.server.resp.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ReplayingDecoder;
import kiwi.server.resp.command.CommandType;
import kiwi.server.resp.command.RESPCommand;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

public class RESPDecoder extends ReplayingDecoder<RESPDecoder.State> {
    private Deque<String> arguments;

    enum State {
        READ_INITIAL,
        READ_COMMAND,
    }

    public RESPDecoder() {
        super(State.READ_INITIAL);
    }

    @Override
    protected void decode(ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out) throws IllegalArgumentException {
        switch (state()) {
            case READ_INITIAL -> {
                char firstChar = (char) in.readByte();
                if (firstChar == '\n' || firstChar == '\r') {
                    return;
                }
                if (firstChar != '*') {
                    throw new IllegalArgumentException("Invalid RESP message: expected '*' as first char, got '" + firstChar + "'");
                }
               
                arguments = new ArrayDeque<>();

                checkpoint(State.READ_COMMAND);
            }
            case READ_COMMAND -> {
                int argumentCount = readInteger(in);
                if (argumentCount < 1) {
                    throw new IllegalArgumentException("Invalid RESP message: expected at least one argument, got " + argumentCount);
                }

                for (int i = 0; i < argumentCount; i++) {
                    arguments.add(readBulkString(in));
                }

                String rawCommand = arguments.removeFirst().toUpperCase();
                CommandType commandType = parseCommandType(rawCommand);

                RESPCommand command = new RESPCommand(commandType, arguments.stream().toList());
                out.add(command);

                checkpoint(State.READ_INITIAL);
            }
        }
    }

    private int readInteger(ByteBuf byteBuf) {
        StringBuilder sb = new StringBuilder();
        char c;
        while ((c = (char) byteBuf.readByte()) != '\r') {
            sb.append(c);
        }
        byteBuf.skipBytes(1); // Skip '\n'
        return Integer.parseInt(sb.toString());
    }

    private String readBulkString(ByteBuf in) {
        in.skipBytes(1); // Skip '$'
        int length = readInteger(in);
        if (length == -1) {
            return null;
        }
        byte[] bytes = new byte[length];
        in.readBytes(bytes);
        in.skipBytes(2); // Skip '\r\n'
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private CommandType parseCommandType(String command) {
        try {
            return CommandType.valueOf(command);
        } catch (IllegalArgumentException e) {
            return CommandType.UNKNOWN;
        }
    }
}
