package kiwi.common;

import java.util.Arrays;

public class Bytes {

    public static final Bytes EMPTY = new Bytes(new byte[0]);

    private static final char[] HEX_CHARS_UPPER = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private final byte[] bytes;

    Bytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public static Bytes wrap(String str) {
        return wrap(str.getBytes());
    }

    public static Bytes wrap(byte[] bytes) {
        if (bytes == null)
            return null;
        return new Bytes(bytes);
    }

    public byte[] get() {
        return this.bytes;
    }

    public int size() {
        return bytes.length;
    }


    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;
        if (other == null)
            return false;
        if (other instanceof Bytes)
            return Arrays.equals(this.bytes, ((Bytes) other).get());
        return false;
    }

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg: \x00 \x05 etc.
     *
     * @return string output
     */
    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();

        if (bytes == null)
            return result.toString();

        for (byte b : bytes) {
            int ch = b & 0xFF;
            if (ch >= ' ' && ch <= '~' && ch != '\\') {
                result.append((char) ch);
            } else {
                result.append("\\x");
                result.append(HEX_CHARS_UPPER[ch / 0x10]);
                result.append(HEX_CHARS_UPPER[ch % 0x10]);
            }
        }
        return result.toString();
    }
}
