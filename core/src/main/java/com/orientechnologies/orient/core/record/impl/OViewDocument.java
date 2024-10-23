package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.OElement;

public class OViewDocument extends ODocument {

  private final OView view;

  public OViewDocument(ODatabaseDocumentInternal database, ORID rid) {
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
  public void setPropertyWithoutValidation(String name, Object value) {
    checkForBinding();
    super.setPropertyWithoutValidation(name, value);

    if (view != null && view.isUpdatable()) {
      String originField = view.getOriginRidField();
      if (originField != null) {
        Object origin = getPropertyWithoutValidation(originField);
        if (origin instanceof ORID) {
          origin = ((ORID) origin).getRecord();
        }
        if (origin instanceof OElementInternal) {
          ((OElementInternal) origin).setPropertyWithoutValidation(name, value);
        }
      }
    }
  }
}
