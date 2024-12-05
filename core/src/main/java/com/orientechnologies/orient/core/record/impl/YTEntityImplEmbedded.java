package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTVertex;
import java.util.Optional;
import javax.annotation.Nullable;

public class YTEntityImplEmbedded extends YTEntityImpl {

  public YTEntityImplEmbedded() {
    super();
  }

  public YTEntityImplEmbedded(String clazz) {
    super(clazz);
    checkEmbeddable();
  }

  public YTEntityImplEmbedded(String clazz, YTDatabaseSessionInternal session) {
    super(session, clazz);
    checkEmbeddable();
  }

  public YTEntityImplEmbedded(YTDatabaseSessionInternal session) {
    super(session);
  }

  @Override
  public boolean isEmbedded() {
    return true;
  }

  @Override
  public YTEntityImplEmbedded copy() {
    var doc = new YTEntityImplEmbedded();
    ORecordInternal.unsetDirty(doc);
    var newDoc = (YTEntityImplEmbedded) copyTo(doc);
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
  public YTEntityImpl reset() {
    throw new UnsupportedOperationException("Cannot reset embedded document");
  }
}
