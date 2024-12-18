package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.type.EntityWrapper;

/**
 *
 */
public abstract class Identity extends EntityWrapper {

  public static final String CLASS_NAME = "OIdentity";

  public Identity() {
  }

  public Identity(DatabaseSessionInternal db, String iClassName) {
    super(db, iClassName);
  }

}
