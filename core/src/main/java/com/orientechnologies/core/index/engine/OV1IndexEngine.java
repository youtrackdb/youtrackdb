package com.orientechnologies.core.index.engine;

import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import java.util.stream.Stream;

public interface OV1IndexEngine extends OBaseIndexEngine {

  int API_VERSION = 1;

  void put(OAtomicOperation atomicOperation, Object key, YTRID value);

  Stream<YTRID> get(Object key);

  @Override
  default int getEngineAPIVersion() {
    return API_VERSION;
  }

  boolean isMultiValue();
}
