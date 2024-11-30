package kiwi.server.resp.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class RESPEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws IllegalArgumentException {
        encode(msg, out);
    }

    private void encode(Object msg, ByteBuf out) throws IllegalArgumentException {
        switch (msg) {
            case String simple -> encodeSimpleString(simple, out);
            case Integer num -> encodeNumber(num, out);
            case Long num -> encodeNumber(num, out);
            case byte[] bytes -> encodeBulkString(bytes, out);
            case String[] strings -> encodeArray(strings, out);
            case List<?> list -> encodeList(list, out);
            case Map<?, ?> map -> encodeMap(map, out);
            case Throwable throwable -> encodeError(throwable, out);
            default -> throw new IllegalArgumentException("Unsupported message type: " + msg.getClass().getName());
        }
    }

    private void encodeSimpleString(String msg, ByteBuf out) {
        out.writeByte('+');
        out.writeCharSequence(msg, java.nio.charset.StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
    }

    private <T> void encodeNumber(T msg, ByteBuf out) {
        out.writeByte(':');
        out.writeCharSequence(msg.toString(), java.nio.charset.StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
    }

    private void encodeBulkString(byte[] msg, ByteBuf out) {
        if (msg.length == 0) { // Null
            out.writeBytes("$-1\r\n".getBytes(StandardCharsets.UTF_8));
        } else {
            out.writeByte('$');
            out.writeCharSequence(String.valueOf(msg.length), StandardCharsets.UTF_8);
            out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
            out.writeBytes(msg);
            out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void encodeArray(String[] msg, ByteBuf out) {
        out.writeByte('*');
        out.writeCharSequence(String.valueOf(msg.length), StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        for (String element : msg) {
            if (element == null) {
                encodeBulkString(new byte[0], out);
            } else {
                encodeBulkString(element.getBytes(StandardCharsets.UTF_8), out);
            }
        }
    }

    private void encodeList(List<?> list, ByteBuf out) {
        out.writeByte('*');
        out.writeCharSequence(String.valueOf(list.size()), StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        for (Object element : list) {
            encode(element, out);
        }
    }

    private void encodeMap(Map<?, ?> map, ByteBuf out) {
        out.writeByte('*');
        out.writeCharSequence(String.valueOf(map.size() * 2), StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            encodeBulkString(entry.getKey().toString().getBytes(StandardCharsets.UTF_8), out);
            encodeBulkString(entry.getValue().toString().getBytes(StandardCharsets.UTF_8), out);
        }
    }

    private void encodeError(Throwable error, ByteBuf out) {
        out.writeByte('-');
        out.writeCharSequence("ERR " + error.getMessage(), StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
    }
}
