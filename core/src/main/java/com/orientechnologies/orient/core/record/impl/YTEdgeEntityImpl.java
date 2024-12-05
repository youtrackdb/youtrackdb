package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import java.util.Set;
import javax.annotation.Nullable;

public class YTEdgeEntityImpl extends YTEntityImpl implements YTEdgeInternal {

  public YTEdgeEntityImpl(YTDatabaseSessionInternal session, String cl) {
    super(session, cl);
  }

  public YTEdgeEntityImpl() {
    super();
  }

  public YTEdgeEntityImpl(YTDatabaseSessionInternal session) {
    super(session);
  }

  public YTEdgeEntityImpl(YTDatabaseSessionInternal database, YTRID rid) {
    super(database, rid);
  }

  @Override
  public YTVertex getFrom() {
    Object result = getPropertyInternal(DIRECTION_OUT);
    if (!(result instanceof YTEntity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.toVertex();
  }

  @Override
  @Nullable
  public YTIdentifiable getFromIdentifiable() {
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
  public YTVertex getTo() {
    Object result = getPropertyInternal(DIRECTION_IN);
    if (!(result instanceof YTEntity v)) {
      return null;
    }

    if (!v.isVertex()) {
      return null;
    }

    return v.toVertex();
  }

  @Override
  public YTIdentifiable getToIdentifiable() {
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
    // LIGHTWEIGHT EDGES MANAGED BY YTEdgeDelegate, IN FUTURE MAY BE WE NEED TO HANDLE THEM WITH THIS
    return false;
  }

  public void delete() {
    checkForBinding();

    super.delete();
  }

  @Override
  @Nullable
  public YTEntityImpl getBaseDocument() {
    return this;
  }

  @Override
  public YTEdgeEntityImpl copy() {
    checkForBinding();

    return (YTEdgeEntityImpl) super.copyTo(new YTEdgeEntityImpl());
  }

  @Override
  public Set<String> getPropertyNames() {
    checkForBinding();

    return YTEdgeInternal.filterPropertyNames(super.getPropertyNames());
  }

  @Override
  public <RET> RET getProperty(String fieldName) {
    checkForBinding();

    YTEdgeInternal.checkPropertyName(fieldName);

    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public YTIdentifiable getLinkProperty(String fieldName) {
    checkForBinding();

    YTEdgeInternal.checkPropertyName(fieldName);

    return super.getLinkProperty(fieldName);
  }

  @Override
  public void setProperty(String fieldName, Object propertyValue) {
    checkForBinding();

    YTEdgeInternal.checkPropertyName(fieldName);

    setPropertyInternal(fieldName, propertyValue);
  }

  @Override
  public void setProperty(String name, Object value, YTType... types) {
    checkForBinding();
    YTEdgeInternal.checkPropertyName(name);

    super.setProperty(name, value, types);
  }

  @Override
  public <RET> RET removeProperty(String fieldName) {
    checkForBinding();
    YTEdgeInternal.checkPropertyName(fieldName);

    return removePropertyInternal(fieldName);
  }

  @Override
  public void promoteToRegularEdge() {
    checkForBinding();
  }

  public static void deleteLinks(YTEdge delegate) {
    YTVertex from = delegate.getFrom();
    if (from != null) {
      YTVertexInternal.removeOutgoingEdge(from, delegate);
    }
    YTVertex to = delegate.getTo();
    if (to != null) {
      YTVertexInternal.removeIncomingEdge(to, delegate);
    }
  }
}
