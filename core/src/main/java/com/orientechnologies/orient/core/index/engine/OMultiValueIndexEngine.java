package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;

public interface OMultiValueIndexEngine extends OV1IndexEngine {

  boolean remove(OAtomicOperation atomicOperation, Object key, YTRID value);

  @Override
  default boolean isMultiValue() {
    return true;
  }
}
