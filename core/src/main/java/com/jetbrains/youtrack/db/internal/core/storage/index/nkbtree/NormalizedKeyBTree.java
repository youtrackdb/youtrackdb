package com.jetbrains.youtrack.db.internal.core.storage.index.nkbtree;

import com.jetbrains.youtrack.db.internal.core.index.OCompositeKey;

public interface NormalizedKeyBTree<K> {

  byte[] get(final OCompositeKey key);

  void put(final OCompositeKey key, final byte[] value);
}
