package com.orientechnologies.orient.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

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
