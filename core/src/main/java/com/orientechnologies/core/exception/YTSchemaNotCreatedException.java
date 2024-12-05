package com.orientechnologies.core.exception;

import com.orientechnologies.common.exception.YTHighLevelException;

public class YTSchemaNotCreatedException extends YTSchemaException implements YTHighLevelException {

  public YTSchemaNotCreatedException(String message) {
    super(message);
  }

  /**
   * This constructor is needed to restore and reproduce exception on client side in case of remote
   * storage exception handling. Please create "copy constructor" for each exception which has
   * current one as a parent.
   *
   * @param exception
   */
  public YTSchemaNotCreatedException(YTSchemaNotCreatedException exception) {
    super(exception);
  }
}
