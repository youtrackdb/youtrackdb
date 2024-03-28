package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.ORecordAbstract;
import com.orientechnologies.orient.core.record.ORecordInternal;
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
  public void convertToProxyRecord(ORecordAbstract primaryRecord) {
    if (!(primaryRecord instanceof OVertexDocument) && !((ODocument) primaryRecord).isVertex()) {
      throw new IllegalArgumentException("Can't convert to a proxy of OVertexDocument");
    }

    super.convertToProxyRecord(primaryRecord);
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OVertexInternal) primaryRecord).getPropertyNames();
    }

    return OVertexInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OVertexInternal) primaryRecord).getProperty(fieldName);
    }

    OVertexInternal.checkPropertyName(fieldName);

    return getPropertyWithoutValidation(fieldName);
  }

  @Nullable
  @Override
  public OIdentifiable getLinkProperty(String fieldName) {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OVertexInternal) primaryRecord).getLinkProperty(fieldName);
    }

    OVertexInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    checkForLoading();
    if (primaryRecord != null) {
      ((OVertexInternal) primaryRecord).setProperty(fieldName, propertyValue);
      return;
    }

    OVertexInternal.checkPropertyName(fieldName);

    setPropertyWithoutValidation(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object value, OType... types) {
    checkForLoading();
    if (primaryRecord != null) {
      ((OVertexInternal) primaryRecord).setProperty(name, value, types);
      return;
    }

    OVertexInternal.checkPropertyName(name);

    super.setProperty(name, value, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OVertexInternal) primaryRecord).removeProperty(fieldName);
    }

    OVertexInternal.checkPropertyName(fieldName);

    return removePropertyWithoutValidation(fieldName);
  }

  @Override
  public Iterable<OVertex> getVertices(ODirection direction) {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OVertexInternal) primaryRecord).getVertices(direction);
    }

    return OVertexInternal.super.getVertices(direction);
  }

  @Override
  public OVertexDocument delete() {
    checkForLoading();
    if (primaryRecord != null) {
      ((OVertexInternal) primaryRecord).delete();
      return (OVertexDocument) primaryRecord;
    }

    super.delete();
    return this;
  }

  @Override
  public OVertexDocument copy() {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OVertexDocument) primaryRecord).copy();
    }

    var newDoc = new OVertexDocument();
    ORecordInternal.unsetDirty(newDoc);
    copyTo(newDoc);
    newDoc.dirty = true;

    return newDoc;
  }

  @Override
  @Nonnull
  public ODocument getBaseDocument() {
    checkForLoading();
    if (primaryRecord != null) {
      return (ODocument) primaryRecord;
    }

    return this;
  }
}
