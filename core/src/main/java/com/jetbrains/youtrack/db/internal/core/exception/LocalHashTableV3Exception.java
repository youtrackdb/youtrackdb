package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v3.LocalHashTableV3;

/**
 * @since 10/2/2015
 */
public class LocalHashTableV3Exception extends DurableComponentException {

  public LocalHashTableV3Exception(LocalHashTableV3Exception exception) {
    super(exception);
  }

  public LocalHashTableV3Exception(String message, LocalHashTableV3 component) {
    super(message, component);
  }
}
