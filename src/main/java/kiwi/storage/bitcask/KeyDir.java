package kiwi.storage.bitcask;

import kiwi.common.Bytes;
import kiwi.storage.bitcask.log.LogSegment;
import kiwi.storage.bitcask.log.Record;

import java.util.concurrent.ConcurrentHashMap;

public class KeyDir extends ConcurrentHashMap<Bytes, ValueReference> {

    public void update(Record record, LogSegment segment) {
        if (record.isTombstone()) {
            super.remove(record.key());
        } else {
            long offset = segment.position() - record.valueSize();
            // TODO: Avoid creating read-only segment for each put request.
            ValueReference valueRef = ValueReference.of(segment, offset, record);
            super.put(record.key(), valueRef);
        }
    }

}
