package kiwi.storage.bitcask.log;

import kiwi.common.Bytes;
import kiwi.storage.bitcask.Header;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RecordTest {

    @Test
    void testIsValidChecksumWithValidHeader() {
        Record record = Record.of(Bytes.wrap("key"), Bytes.wrap("value"));
        assertTrue(record.isValidChecksum());
    }

    @Test
    void testIsValidChecksumWithInvalidHeader() {
        Header header = new Header(0, 0, 0, 0, 0);
        Record record = new Record(header, Bytes.wrap("key"), Bytes.wrap("value"));
        assertFalse(record.isValidChecksum());
    }

    @Test
    void testIsTombstone() {
        assertTrue(Record.of(Bytes.wrap("k"), Bytes.wrap(new byte[0])).isTombstone());
        assertFalse(Record.of(Bytes.wrap("k"), Bytes.wrap("v")).isTombstone());
    }

}