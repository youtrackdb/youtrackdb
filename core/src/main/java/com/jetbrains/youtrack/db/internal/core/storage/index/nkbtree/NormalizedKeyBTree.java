package com.jetbrains.youtrack.db.internal.core.storage.index.nkbtree;

import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;

public interface NormalizedKeyBTree<K> {

  byte[] get(final CompositeKey key);

  void put(final CompositeKey key, final byte[] value);
}
