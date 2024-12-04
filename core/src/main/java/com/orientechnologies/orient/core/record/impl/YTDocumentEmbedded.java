package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.YTVertex;
import java.util.Optional;
import javax.annotation.Nullable;

public class YTDocumentEmbedded extends YTDocument {

  public YTDocumentEmbedded() {
    super();
  }

  public YTDocumentEmbedded(String clazz) {
    super(clazz);
    checkEmbeddable();
  }

  public YTDocumentEmbedded(String clazz, YTDatabaseSessionInternal session) {
    super(session, clazz);
    checkEmbeddable();
  }

  public YTDocumentEmbedded(YTDatabaseSessionInternal session) {
    super(session);
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

  @Override
  public YTDocumentEmbedded copy() {
    var doc = new YTDocumentEmbedded();
    ORecordInternal.unsetDirty(doc);
    var newDoc = (YTDocumentEmbedded) copyTo(doc);
    newDoc.dirty = true;
    return newDoc;
  }

  @Override
  public void save() {
    throw new UnsupportedOperationException("Cannot save embedded document");
  }

  @Override
  public Optional<YTVertex> asVertex() {
    return Optional.empty();
  }

  @Override
  @Nullable
  public YTVertex toVertex() {
    return null;
  }

  @Override
  public Optional<YTEdge> asEdge() {
    return Optional.empty();
  }

  @Override
  @Nullable
  public YTEdge toEdge() {
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
  public void unload() {
    throw new UnsupportedOperationException("Cannot unload embedded document");
  }

  @Override
  public YTDocument reset() {
    throw new UnsupportedOperationException("Cannot reset embedded document");
  }
}
