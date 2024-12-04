package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.type.ODocumentWrapper;

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

  public OIdentity(YTDocument iDocument) {
    super(iDocument);
  }
}
