package com.jetbrains.youtrack.db.internal.core.exception;

import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.base.ODurableComponent;

/**
 * @since 10/2/2015
 */
public abstract class YTDurableComponentException extends YTCoreException {

  public YTDurableComponentException(YTDurableComponentException exception) {
    super(exception);
  }

  public YTDurableComponentException(String message, ODurableComponent component) {
    super(message, component.getName());
  }
}
