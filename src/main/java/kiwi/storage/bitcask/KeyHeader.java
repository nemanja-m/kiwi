package kiwi.storage.bitcask;

import kiwi.common.Bytes;

public record KeyHeader(Bytes key, Header header) {
}
