package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Optional;
import javax.annotation.Nullable;

public class ODocumentEmbedded extends ODocument {
  public ODocumentEmbedded() {
    super();
  }

  public ODocumentEmbedded(String clazz) {
    super(clazz);
    checkEmbeddable();
  }

  public ODocumentEmbedded(String clazz, ODatabaseSessionInternal session) {
    super(session, clazz);
    checkEmbeddable();
  }

  public ODocumentEmbedded(ODatabaseSessionInternal session) {
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

  @Override
  public void save() {
    throw new UnsupportedOperationException("Cannot save embedded document");
  }

  @Override
  public Optional<OVertex> asVertex() {
    return Optional.empty();
  }

  @Override
  @Nullable
  public OVertex toVertex() {
    return null;
  }

  @Override
  public Optional<OEdge> asEdge() {
    return Optional.empty();
  }

  @Override
  @Nullable
  public OEdge toEdge() {
    return null;
  }

  @Override
  public boolean isVertex() {
    return false;
  }

  @Override
  public boolean isEdge() {
    return false;
  }

  @Override
  public ODocument load(String iFetchPlan) {
    throw new UnsupportedOperationException("Cannot load embedded document");
  }

  @Override
  public ODocument load(String iFetchPlan, boolean iIgnoreCache) {
    return super.load(iFetchPlan, iIgnoreCache);
  }

  @Override
  public void unload() {
    throw new UnsupportedOperationException("Cannot unload embedded document");
  }

  @Override
  public ODocument reset() {
    throw new UnsupportedOperationException("Cannot reset embedded document");
  }
}
