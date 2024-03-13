package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OVertexDocument extends ODocument implements OVertexInternal {

  public OVertexDocument() {
    super();
  }

  public OVertexDocument(ODatabaseSession session) {
    super(session);
  }

  public OVertexDocument(ODatabaseSession database, ORID rid) {
    super(database, rid);
  }

  public OVertexDocument(ODatabaseSession session, String klass) {
    super(session, klass);
    if (!getImmutableSchemaClass().isVertexType()) {
      throw new IllegalArgumentException(getClassName() + " is not a vertex class");
    }
  }

  @Override
  public Set<String> getPropertyNames() {
    return OVertexInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    OVertexInternal.checkPropertyName(fieldName);

    return getPropertyWithoutValidation(fieldName);
  }

  @Nullable
  @Override
  public OIdentifiable getLinkProperty(String fieldName) {
    OVertexInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    OVertexInternal.checkPropertyName(fieldName);

    setPropertyWithoutValidation(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object value, OType... types) {
    OVertexInternal.checkPropertyName(name);

    super.setProperty(name, value, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    OVertexInternal.checkPropertyName(fieldName);

    return removePropertyWithoutValidation(fieldName);
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction) {
    return OVertexInternal.super.getVertices(direction);
  }

  @Override
  public OVertexDocument delete() {
    super.delete();
    return this;
  }

  @Override
  public OVertexDocument copy() {
    return (OVertexDocument) copyTo(new OVertexDocument());
  }

  @Override
  @Nonnull
  public ODocument getBaseDocument() {
    return this;
  }
}
