package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.common.exception.YTHighLevelException;

/**
 * This exception is thrown if access to the manager of tree-based RidBags will be prohibited.
 */
public class YTAccessToSBtreeCollectionManagerIsProhibitedException extends YTCoreException
    implements YTHighLevelException {

  public YTAccessToSBtreeCollectionManagerIsProhibitedException(
      YTAccessToSBtreeCollectionManagerIsProhibitedException exception) {
    super(exception);
  }

  public YTAccessToSBtreeCollectionManagerIsProhibitedException(String message) {
    super(message);
  }
}
