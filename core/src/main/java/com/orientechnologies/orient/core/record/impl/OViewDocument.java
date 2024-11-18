package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.OElement;

public class OViewDocument extends ODocument {

  private OView view;

  public OViewDocument(ODatabaseSessionInternal database, int cluster) {
    view = database.getViewFromCluster(cluster);
  }

  public OViewDocument(ODatabaseSessionInternal database, ORID rid) {
    super(database, rid);
    view = database.getViewFromCluster(rid.getClusterId());
  }

  @Override
  public OView getSchemaClass() {
    checkForBinding();

    return view;
  }

  @Override
  protected OImmutableClass getImmutableSchemaClass() {
    checkForBinding();

    if (view instanceof OImmutableClass) {
      return (OImmutableClass) view;
    } else {
      return (OImmutableClass) getImmutableSchema().getView(view.getName());
    }
  }

  @Override
  public String getClassName() {
    OView clazz = getSchemaClass();
    return clazz == null ? null : clazz.getName();
  }

  @Override
  public void setProperty(String iFieldName, Object iPropertyValue) {
    checkForBinding();

    super.setProperty(iFieldName, iPropertyValue);
    if (view != null && view.isUpdatable()) {
      String originField = view.getOriginRidField();
      if (originField != null) {
        Object origin = getProperty(originField);
        if (origin instanceof ORID) {
          origin = ((ORID) origin).getRecord();
        }
        if (origin instanceof OElement) {
          ((OElement) origin).setProperty(iFieldName, iPropertyValue);
        }
      }
    }
  }

  @Override
  public void setPropertyInternal(String name, Object value) {
    checkForBinding();
    super.setPropertyInternal(name, value);

    if (view != null && view.isUpdatable()) {
      String originField = view.getOriginRidField();
      if (originField != null) {
        Object origin = getPropertyInternal(originField);
        if (origin instanceof ORID) {
          origin = ((ORID) origin).getRecord();
        }
        if (origin instanceof OElementInternal) {
          ((OElementInternal) origin).setPropertyInternal(name, value);
        }
      }
    }
  }
}
