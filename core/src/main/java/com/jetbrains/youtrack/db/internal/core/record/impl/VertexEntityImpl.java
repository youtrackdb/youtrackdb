package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class VertexEntityImpl extends EntityImpl implements VertexInternal {

  public VertexEntityImpl() {
    super();
  }

  public VertexEntityImpl(YTDatabaseSessionInternal database, YTRID rid) {
    super(database, rid);
  }

  public VertexEntityImpl(YTDatabaseSessionInternal session, String klass) {
    super(session, klass);
    if (!getImmutableSchemaClass().isVertexType()) {
      throw new IllegalArgumentException(getClassName() + " is not a vertex class");
    }
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForBinding();

    return EdgeInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    checkForBinding();

    EdgeInternal.checkPropertyName(fieldName);

    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public YTIdentifiable getLinkProperty(String fieldName) {
    checkForBinding();

    EdgeInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    checkForBinding();

    EdgeInternal.checkPropertyName(fieldName);

    setPropertyInternal(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object value, YTType... types) {
    checkForBinding();

    EdgeInternal.checkPropertyName(name);

    super.setProperty(name, value, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForBinding();
    EdgeInternal.checkPropertyName(fieldName);

    return removePropertyInternal(fieldName);
  }

  @Override
  public Iterable<Vertex> getVertices(ODirection direction) {
    checkForBinding();
    return VertexInternal.super.getVertices(direction);
  }

  @Override
  public void delete() {
    checkForBinding();

    super.delete();
  }

  @Override
  public VertexEntityImpl copy() {
    checkForBinding();

    var newDoc = new VertexEntityImpl();
    ORecordInternal.unsetDirty(newDoc);
    copyTo(newDoc);
    newDoc.dirty = true;

    return newDoc;
  }

  @Override
  @Nonnull
  public EntityImpl getBaseDocument() {
    return this;
  }
}
