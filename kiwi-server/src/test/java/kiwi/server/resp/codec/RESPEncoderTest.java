package kiwi.server.resp.codec;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.EncoderException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RESPEncoderTest {

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        channel = new EmbeddedChannel(new RESPEncoder());
    }

    @Test
    void testEncodeSimpleString() {
        String message = "OK";
        channel.writeOutbound(message);
        ByteBuf encoded = channel.readOutbound();
        assertEquals("+OK\r\n", encoded.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testEncodeNumber() {
        int number = 123;
        channel.writeOutbound(number);
        ByteBuf encoded = channel.readOutbound();
        assertEquals(":123\r\n", encoded.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testEncodeBulkString() {
        byte[] message = "Hello".getBytes(StandardCharsets.UTF_8);
        channel.writeOutbound(message);
        ByteBuf encoded = channel.readOutbound();
        assertEquals("$5\r\nHello\r\n", encoded.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testEncodeEmptyBulkString() {
        byte[] message = new byte[0];
        channel.writeOutbound(message);
        ByteBuf encoded = channel.readOutbound();
        assertEquals("$-1\r\n", encoded.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testEncodeArray() {
        String[] messages = {"Hello", "World"};
        channel.writeOneOutbound(messages);
        channel.flush();
        ByteBuf encoded = channel.readOutbound();
        assertEquals("*2\r\n$5\r\nHello\r\n$5\r\nWorld\r\n", encoded.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testEncodeError() {
        Throwable error = new IllegalArgumentException("Error message");
        channel.writeOutbound(error);
        ByteBuf encoded = channel.readOutbound();
        assertEquals("-ERR Error message\r\n", encoded.toString(StandardCharsets.UTF_8));
    }

    @Test
    void testEncodeUnsupportedType() {
        Object unsupported = new Object();
        assertThrows(EncoderException.class, () -> channel.writeOutbound(unsupported));
    }
}