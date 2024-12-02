package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Set;
import javax.annotation.Nullable;

public class OEdgeDocument extends ODocument implements OEdgeInternal {

  public OEdgeDocument(ODatabaseSessionInternal session, String cl) {
    super(session, cl);
  }

  public OEdgeDocument() {
    super();
  }

  public OEdgeDocument(ODatabaseSessionInternal session) {
    super(session);
  }

  public OEdgeDocument(ODatabaseSessionInternal database, ORID rid) {
    super(database, rid);
  }

  @Override
  public OVertex getFrom() {
    Object result = getPropertyInternal(DIRECTION_OUT);
    if (!(result instanceof OElement v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.toVertex();
  }

  @Override
  @Nullable
  public OIdentifiable getFromIdentifiable() {
    var db = getSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    var result = getLinkPropertyInternal(DIRECTION_OUT);
    if (result == null) {
      return null;
    }

    var rid = result.getIdentity();
    if (schema.getClassByClusterId(rid.getClusterId()).isVertexType()) {
      return rid;
    }

    return null;
  }

  @Override
  public OVertex getTo() {
    Object result = getPropertyInternal(DIRECTION_IN);
    if (!(result instanceof OElement v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.toVertex();
  }

  @Override
  public OIdentifiable getToIdentifiable() {
    var db = getSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    var result = getLinkPropertyInternal(DIRECTION_IN);
    if (result == null) {
      return null;
    }

    var rid = result.getIdentity();
    if (schema.getClassByClusterId(rid.getClusterId()).isVertexType()) {
      return rid;
    }

    return null;
  }

  @Override
  public boolean isLightweight() {
    // LIGHTWEIGHT EDGES MANAGED BY OEdgeDelegate, IN FUTURE MAY BE WE NEED TO HANDLE THEM WITH THIS
    return false;
  }

  public void delete() {
    checkForBinding();

    super.delete();
  }

  @Override
  @Nullable
  public ODocument getBaseDocument() {
    return this;
  }

  @Override
  public OEdgeDocument copy() {
    checkForBinding();

    return (OEdgeDocument) super.copyTo(new OEdgeDocument());
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForBinding();

    return OEdgeInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    checkForBinding();

    OEdgeInternal.checkPropertyName(fieldName);

    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public OIdentifiable getLinkProperty(String fieldName) {
    checkForBinding();

    OEdgeInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    checkForBinding();

    OEdgeInternal.checkPropertyName(fieldName);

    setPropertyInternal(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object value, OType... types) {
    checkForBinding();
    OEdgeInternal.checkPropertyName(name);

    super.setProperty(name, value, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForBinding();
    OEdgeInternal.checkPropertyName(fieldName);

    return removePropertyInternal(fieldName);
  }

  @Override
  public void promoteToRegularEdge() {
    checkForBinding();
  }

  public static void deleteLinks(OEdge delegate) {
    OVertex from = delegate.getFrom();
    if (from != null) {
      OVertexInternal.removeOutgoingEdge(from, delegate);
    }
    OVertex to = delegate.getTo();
    if (to != null) {
      OVertexInternal.removeIncomingEdge(to, delegate);
    }
  }
}
