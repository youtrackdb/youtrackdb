package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.type.EntityWrapper;

/**
 *
 */
public abstract class Identity extends EntityWrapper {

  public static final String CLASS_NAME = "OIdentity";

  public Identity() {
  }

  public Identity(RID iRID) {
    super(iRID.getRecord());
  }

  public Identity(String iClassName) {
    super(iClassName);
  }

  public Identity(EntityImpl iDocument) {
    super(iDocument);
  }
}
