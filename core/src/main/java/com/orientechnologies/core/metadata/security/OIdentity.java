package com.orientechnologies.core.metadata.security;

import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.type.ODocumentWrapper;

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

  public OIdentity(YTEntityImpl iDocument) {
    super(iDocument);
  }
}
