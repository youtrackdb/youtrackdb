package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.OSBTreeBonsaiLocal;

/**
 * @since 10/2/2015
 */
public class OSBTreeBonsaiLocalException extends YTDurableComponentException {

  public OSBTreeBonsaiLocalException(OSBTreeBonsaiLocalException exception) {
    super(exception);
  }

  public OSBTreeBonsaiLocalException(String message, OSBTreeBonsaiLocal component) {
    super(message, component);
  }
}
