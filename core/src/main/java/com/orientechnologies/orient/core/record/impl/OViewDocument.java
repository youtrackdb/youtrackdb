package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecordAbstract;

public class OViewDocument extends ODocument {

  private OView view;

  public OViewDocument(ODatabaseDocumentInternal database, int cluster) {
    view = database.getViewFromCluster(cluster);
  }

  @Override
  public void convertToProxyRecord(ORecordAbstract primaryRecord) {
    if (!(primaryRecord instanceof OViewDocument)) {
      throw new IllegalArgumentException("Can't convert to a proxy of OViewDocument");
    }

    super.convertToProxyRecord(primaryRecord);
    view = null;
  }

  @Override
  public OView getSchemaClass() {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OViewDocument) primaryRecord).getSchemaClass();
    }

    return view;
  }

  @Override
  protected OImmutableClass getImmutableSchemaClass() {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OViewDocument) primaryRecord).getImmutableSchemaClass();
    }

    if (view instanceof OImmutableClass) {
      return (OImmutableClass) view;
    } else {
      return (OImmutableClass) getImmutableSchema().getView(view.getName());
    }
  }

  @Override
  public String getClassName() {
    checkForLoading();
    if (primaryRecord != null) {
      return ((OViewDocument) primaryRecord).getClassName();
    }

    OView clazz = getSchemaClass();
    return clazz == null ? null : clazz.getName();
  }

  @Override
  public void setProperty(String iFieldName, Object iPropertyValue) {
    checkForLoading();
    if (primaryRecord != null) {
      ((OViewDocument) primaryRecord).setProperty(iFieldName, iPropertyValue);
      return;
    }

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
    checkForLoading();
    if (primaryRecord != null) {
      ((OViewDocument) primaryRecord).setPropertyWithoutValidation(name, value);
      return;
    }

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
