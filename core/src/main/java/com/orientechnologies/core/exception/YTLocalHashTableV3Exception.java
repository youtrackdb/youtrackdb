package com.orientechnologies.core.exception;

import com.orientechnologies.core.storage.index.hashindex.local.v3.OLocalHashTableV3;

/**
 * @since 10/2/2015
 */
public class YTLocalHashTableV3Exception extends YTDurableComponentException {

  public YTLocalHashTableV3Exception(YTLocalHashTableV3Exception exception) {
    super(exception);
  }

  public YTLocalHashTableV3Exception(String message, OLocalHashTableV3 component) {
    super(message, component);
  }
}
