package kiwi.server.resp.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.nio.charset.StandardCharsets;

public class RESPEncoder extends MessageToByteEncoder<Object> {
    @Override
    protected void encode(ChannelHandlerContext ctx, Object msg, ByteBuf out) throws IllegalArgumentException {
        switch (msg) {
            case String simple -> encodeSimpleString(simple, out);
            case Integer num -> encodeNumber(num, out);
            case Long num -> encodeNumber(num, out);
            case byte[] bytes -> encodeBulkString(bytes, out);
            case String[] strings -> encodeArray(strings, out);
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
            out.writeCharSequence(String.valueOf(msg.length), java.nio.charset.StandardCharsets.UTF_8);
            out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
            out.writeBytes(msg);
            out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        }
    }

    private void encodeArray(String[] msg, ByteBuf out) {
        out.writeByte('*');
        out.writeCharSequence(String.valueOf(msg.length), java.nio.charset.StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
        for (String element : msg) {
            encodeBulkString(element != null ? element.getBytes(StandardCharsets.UTF_8) : null, out);
        }
    }

    private void encodeError(Throwable error, ByteBuf out) {
        out.writeByte('-');
        out.writeCharSequence("ERR " + error.getMessage(), java.nio.charset.StandardCharsets.UTF_8);
        out.writeBytes("\r\n".getBytes(StandardCharsets.UTF_8));
    }
}
