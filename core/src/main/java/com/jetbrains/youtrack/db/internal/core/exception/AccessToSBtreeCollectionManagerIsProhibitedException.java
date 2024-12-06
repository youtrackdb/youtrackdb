package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.HighLevelException;

/**
 * This exception is thrown if access to the manager of tree-based RidBags will be prohibited.
 */
public class AccessToSBtreeCollectionManagerIsProhibitedException extends CoreException
    implements HighLevelException {

  public AccessToSBtreeCollectionManagerIsProhibitedException(
      AccessToSBtreeCollectionManagerIsProhibitedException exception) {
    super(exception);
  }

  public AccessToSBtreeCollectionManagerIsProhibitedException(String message) {
    super(message);
  }
}
