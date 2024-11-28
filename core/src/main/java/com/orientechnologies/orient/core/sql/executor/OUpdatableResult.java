package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.OElementInternal;

/**
 *
 */
public class OUpdatableResult extends OResultInternal {

  protected OResultInternal previousValue = null;

  public OUpdatableResult(ODatabaseSessionInternal session, OElement element) {
    super(session, element);
  }

  @Override
  public boolean isElement() {
    return true;
  }

  public <T> T getProperty(String name) {
    loadIdentifiable();
    T result = null;
    if (content != null && content.containsKey(name)) {
      result = (T) content.get(name);
    } else if (isElement()) {
      result = ((OElement) identifiable).getProperty(name);
    }
    if (result instanceof OIdentifiable && ((OIdentifiable) result).getIdentity().isPersistent()) {
      result = (T) ((OIdentifiable) result).getIdentity();
    }
    return result;
  }

  @Override
  public OElement toElement() {
    return (OElement) identifiable;
  }

  @Override
  public void setProperty(String name, Object value) {
    ((OElementInternal) identifiable).setPropertyInternal(name, value);
  }

  public void removeProperty(String name) {
    ((OElement) identifiable).removeProperty(name);
  }
}
