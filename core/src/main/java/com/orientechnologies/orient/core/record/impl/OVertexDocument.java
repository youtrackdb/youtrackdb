package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class OVertexDocument extends ODocument implements OVertexInternal {

  public OVertexDocument() {
    super();
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
    checkForBinding();

    return OVertexInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    checkForBinding();

    OVertexInternal.checkPropertyName(fieldName);

    return getPropertyWithoutValidation(fieldName);
  }

  @Nullable
  @Override
  public OIdentifiable getLinkProperty(String fieldName) {
    checkForBinding();

    OVertexInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    checkForBinding();

    OVertexInternal.checkPropertyName(fieldName);

    setPropertyWithoutValidation(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object value, OType... types) {
    checkForBinding();

    OVertexInternal.checkPropertyName(name);

    super.setProperty(name, value, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForBinding();
    OVertexInternal.checkPropertyName(fieldName);

    return removePropertyWithoutValidation(fieldName);
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction) {
    checkForBinding();
    return OVertexInternal.super.getVertices(direction);
  }

  @Override
  public OVertexDocument delete() {
    checkForBinding();

    super.delete();
    return this;
  }

  @Override
  public OVertexDocument copy() {
    checkForBinding();

    var newDoc = new OVertexDocument();
    ORecordInternal.unsetDirty(newDoc);
    copyTo(newDoc);
    newDoc.dirty = true;

    return newDoc;
  }

  @Override
  @Nonnull
  public ODocument getBaseDocument() {
    return this;
  }
}
