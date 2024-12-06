package com.jetbrains.youtrack.db.internal.core.index.engine;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;

public interface MultiValueIndexEngine extends V1IndexEngine {

  boolean remove(AtomicOperation atomicOperation, Object key, RID value);

  @Override
  default boolean isMultiValue() {
    return true;
  }
}
