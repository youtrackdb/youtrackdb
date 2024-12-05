package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.type.ODocumentWrapper;

/**
 *
 */
public abstract class OIdentity extends ODocumentWrapper {

  public static final String CLASS_NAME = "OIdentity";

  public OIdentity() {
  }

  public OIdentity(YTRID iRID) {
    super(iRID.getRecord());
  }

  public OIdentity(String iClassName) {
    super(iClassName);
  }

  public OIdentity(EntityImpl iDocument) {
    super(iDocument);
  }
}
