/*
 * Copyright JAMF Software, LLC
 */

package com.jamf.regatta.core.api;

/**
 * A key-value pair together with its MVCC revision metadata.
 *
 * @param key            the key.
 * @param value          the value held by the key.
 * @param createRevision the revision of the last creation of this key, 0 if unknown.
 * @param modRevision    the revision of the last modification of this key, 0 if unknown.
 */
public record KeyValue(ByteSequence key, ByteSequence value, long createRevision, long modRevision) {

    /**
     * Creates a key-value pair without revision metadata (both revisions set to 0).
     *
     * @param key   the key.
     * @param value the value held by the key.
     */
    public KeyValue(ByteSequence key, ByteSequence value) {
        this(key, value, 0L, 0L);
    }

    /**
     * Creates a key-value pair from its protobuf representation, including revision metadata.
     *
     * @param kv the protobuf key-value.
     * @return the key-value pair.
     */
    public static KeyValue from(com.jamf.regatta.proto.KeyValue kv) {
        return new KeyValue(ByteSequence.from(kv.getKey()), ByteSequence.from(kv.getValue()), kv.getCreateRevision(), kv.getModRevision());
    }
}
