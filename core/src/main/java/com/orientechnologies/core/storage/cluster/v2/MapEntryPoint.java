package com.orientechnologies.core.storage.cluster.v2;

import com.orientechnologies.core.storage.cache.OCacheEntry;
import com.orientechnologies.core.storage.impl.local.paginated.base.ODurablePage;

final class MapEntryPoint extends ODurablePage {

  private static final int FILE_SIZE_OFFSET = NEXT_FREE_POSITION;

  MapEntryPoint(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  int getFileSize() {
    return getIntValue(FILE_SIZE_OFFSET);
  }

  void setFileSize(int size) {
    setIntValue(FILE_SIZE_OFFSET, size);
  }
}
