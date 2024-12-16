package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2.LocalHashTableV2;

/**
 * @since 10/2/2015
 */
public class LocalHashTableV2Exception extends DurableComponentException {

  public LocalHashTableV2Exception(LocalHashTableV2Exception exception) {
    super(exception);
  }

  public LocalHashTableV2Exception(String message, LocalHashTableV2 component) {
    super(message, component);
  }
}
