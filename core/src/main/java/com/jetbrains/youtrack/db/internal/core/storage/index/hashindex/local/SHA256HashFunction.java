package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local;

import com.jetbrains.youtrack.db.internal.common.serialization.types.BinarySerializer;
import com.jetbrains.youtrack.db.internal.common.serialization.types.LongSerializer;

public class SHA256HashFunction<V> implements HashFunction<V> {

  private final BinarySerializer<V> valueSerializer;

  public SHA256HashFunction(BinarySerializer<V> valueSerializer) {
    this.valueSerializer = valueSerializer;
  }

  @Override
  public long hashCode(V value) {
    final byte[] serializedValue = new byte[valueSerializer.getObjectSize(value)];
    valueSerializer.serializeNativeObject(value, serializedValue, 0);

    final byte[] digest = MessageDigestHolder.instance().get().digest(serializedValue);
    return LongSerializer.INSTANCE.deserializeNative(digest, 0);
  }
}
