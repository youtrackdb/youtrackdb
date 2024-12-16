package com.jetbrains.youtrack.db.internal.core.storage.index.nkbtree;

import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.DurableComponent;

public class NormalizedKeyBTreeValue<K> extends DurableComponent implements NormalizedKeyBTree<K> {

  private final String nullFileExtension;

  public NormalizedKeyBTreeValue(
      final String name,
      final String dataFileExtension,
      final String nullFileExtension,
      final AbstractPaginatedStorage storage) {
    super(storage, name, dataFileExtension, name + dataFileExtension);
    acquireExclusiveLock();
    try {
      this.nullFileExtension = nullFileExtension;
    } finally {
      releaseExclusiveLock();
    }
  }

  @Override
  public byte[] get(final CompositeKey key) {
    return new byte[0];
  }

  @Override
  public void put(final CompositeKey key, final byte[] value) {
  }
}
