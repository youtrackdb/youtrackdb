package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.record.ORecordInternal;

public class ODocumentEmbedded extends ODocument {

  public ODocumentEmbedded() {
    super();
  }

  public ODocumentEmbedded(String clazz) {
    super(clazz);
    checkEmbeddable();
  }

  public ODocumentEmbedded(String clazz, ODatabaseSession session) {
    super(session, clazz);
    checkEmbeddable();
  }

  public ODocumentEmbedded(ODatabaseSession session) {
    super(session);
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

  @Override
  public ODocumentEmbedded copy() {
    var doc = new ODocumentEmbedded();
    ORecordInternal.unsetDirty(doc);
    var newDoc = (ODocumentEmbedded) copyTo(doc);
    newDoc.dirty = true;
    return newDoc;
  }
}
