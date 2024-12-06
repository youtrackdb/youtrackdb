package com.jetbrains.youtrack.db.internal.core.index.engine;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import java.util.stream.Stream;

public interface V1IndexEngine extends BaseIndexEngine {

  int API_VERSION = 1;

  void put(AtomicOperation atomicOperation, Object key, RID value);

  Stream<RID> get(Object key);

  @Override
  default int getEngineAPIVersion() {
    return API_VERSION;
  }

  boolean isMultiValue();
}
