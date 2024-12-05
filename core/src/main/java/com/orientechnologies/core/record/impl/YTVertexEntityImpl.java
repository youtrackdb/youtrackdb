package com.orientechnologies.core.record.impl;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ODirection;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.YTVertex;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class YTVertexEntityImpl extends YTEntityImpl implements YTVertexInternal {

  public YTVertexEntityImpl() {
    super();
  }

  public YTVertexEntityImpl(YTDatabaseSessionInternal database, YTRID rid) {
    super(database, rid);
  }

  public YTVertexEntityImpl(YTDatabaseSessionInternal session, String klass) {
    super(session, klass);
    if (!getImmutableSchemaClass().isVertexType()) {
      throw new IllegalArgumentException(getClassName() + " is not a vertex class");
    }
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForBinding();

    return YTVertexInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    checkForBinding();

    YTVertexInternal.checkPropertyName(fieldName);

    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public YTIdentifiable getLinkProperty(String fieldName) {
    checkForBinding();

    YTVertexInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    checkForBinding();

    YTVertexInternal.checkPropertyName(fieldName);

    setPropertyInternal(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object value, YTType... types) {
    checkForBinding();

    YTVertexInternal.checkPropertyName(name);

    super.setProperty(name, value, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForBinding();
    YTVertexInternal.checkPropertyName(fieldName);

    return removePropertyInternal(fieldName);
  }

  @Override
  public Iterable<YTVertex> getVertices(ODirection direction) {
    checkForBinding();
    return YTVertexInternal.super.getVertices(direction);
  }

  @Override
  public void delete() {
    checkForBinding();

    super.delete();
  }

  @Override
  public YTVertexEntityImpl copy() {
    checkForBinding();

    var newDoc = new YTVertexEntityImpl();
    ORecordInternal.unsetDirty(newDoc);
    copyTo(newDoc);
    newDoc.dirty = true;

    return newDoc;
  }

  @Override
  @Nonnull
  public YTEntityImpl getBaseDocument() {
    return this;
  }
}
