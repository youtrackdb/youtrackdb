package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.index.versionmap.VersionPositionMap;

/**
 * @since 10/2/2015
 */
public class VersionPositionMapException extends DurableComponentException {

  @SuppressWarnings("unused")
  public VersionPositionMapException(VersionPositionMapException exception) {
    super(exception);
  }

  public VersionPositionMapException(String message, VersionPositionMap component) {
    super(message, component);
  }
}
