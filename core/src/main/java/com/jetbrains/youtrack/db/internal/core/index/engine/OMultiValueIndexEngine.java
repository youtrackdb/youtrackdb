package com.jetbrains.youtrack.db.internal.core.index.engine;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface OMultiValueIndexEngine extends OV1IndexEngine {

  boolean remove(OAtomicOperation atomicOperation, Object key, YTRID value);

  @Override
  default boolean isMultiValue() {
    return true;
  }
}
