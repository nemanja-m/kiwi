package kiwi.server.resp.codec;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.DecoderException;
import kiwi.server.resp.command.CommandType;
import kiwi.server.resp.command.RESPCommand;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class RESPDecoderTest {

    @Test
    void testDecodeValidCommand() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n".getBytes(StandardCharsets.UTF_8));
        EmbeddedChannel channel = new EmbeddedChannel(new RESPDecoder());

        assertTrue(channel.writeInbound(buf));
        RESPCommand command = channel.readInbound();

        assertNotNull(command);
        assertEquals(CommandType.PING, command.commandType());
        assertEquals(List.of("PONG"), command.arguments());
    }

    @Test
    void testDecodeInvalidInitialCharacter() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("!2\r\n$4\r\nPING\r\n$4\r\nPONG\r\n".getBytes(StandardCharsets.UTF_8));
        EmbeddedChannel channel = new EmbeddedChannel(new RESPDecoder());

        assertThrows(DecoderException.class, () -> channel.writeInbound(buf));
    }

    @Test
    void testDecodeInvalidArgumentCount() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*0\r\n".getBytes(StandardCharsets.UTF_8));
        EmbeddedChannel channel = new EmbeddedChannel(new RESPDecoder());

        assertThrows(DecoderException.class, () -> channel.writeInbound(buf));
    }

    @Test
    void testDecodeUnknownCommand() {
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*1\r\n$7\r\nUNKNOWN\r\n".getBytes(StandardCharsets.UTF_8));
        EmbeddedChannel channel = new EmbeddedChannel(new RESPDecoder());

        assertTrue(channel.writeInbound(buf));
        RESPCommand command = channel.readInbound();

        assertNotNull(command);
        assertEquals(CommandType.UNKNOWN, command.commandType());
        assertTrue(command.arguments().isEmpty());
    }
}