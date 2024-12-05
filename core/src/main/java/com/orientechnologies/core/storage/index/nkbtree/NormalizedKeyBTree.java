package com.orientechnologies.core.storage.index.nkbtree;

import com.orientechnologies.core.index.OCompositeKey;

public interface NormalizedKeyBTree<K> {

  byte[] get(final OCompositeKey key);

  void put(final OCompositeKey key, final byte[] value);
}
