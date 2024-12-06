package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.SBTreeBonsaiLocal;

/**
 * @since 10/2/2015
 */
public class SBTreeBonsaiLocalException extends DurableComponentException {

  public SBTreeBonsaiLocalException(SBTreeBonsaiLocalException exception) {
    super(exception);
  }

  public SBTreeBonsaiLocalException(String message, SBTreeBonsaiLocal component) {
    super(message, component);
  }
}
