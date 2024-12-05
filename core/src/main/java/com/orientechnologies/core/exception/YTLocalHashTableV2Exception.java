package com.orientechnologies.core.exception;

import com.orientechnologies.core.storage.index.hashindex.local.v2.LocalHashTableV2;

/**
 * @since 10/2/2015
 */
public class YTLocalHashTableV2Exception extends YTDurableComponentException {

  public YTLocalHashTableV2Exception(YTLocalHashTableV2Exception exception) {
    super(exception);
  }

  public YTLocalHashTableV2Exception(String message, LocalHashTableV2 component) {
    super(message, component);
  }
}
