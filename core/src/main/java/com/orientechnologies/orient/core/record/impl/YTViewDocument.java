package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.YTView;
import com.orientechnologies.orient.core.record.YTEntity;

public class YTViewDocument extends YTDocument {

  private final YTView view;

  public YTViewDocument(YTDatabaseSessionInternal database, int cluster) {
    view = database.getViewFromCluster(cluster);
  }

  public YTViewDocument(YTDatabaseSessionInternal database, YTRID rid) {
    super(database, rid);
    view = database.getViewFromCluster(rid.getClusterId());
  }

  @Override
  public YTView getSchemaClass() {
    checkForBinding();

    return view;
  }

  @Override
  protected YTImmutableClass getImmutableSchemaClass() {
    checkForBinding();

    if (view instanceof YTImmutableClass) {
      return (YTImmutableClass) view;
    } else {
      return (YTImmutableClass) getImmutableSchema().getView(view.getName());
    }
  }

  @Override
  public String getClassName() {
    YTView clazz = getSchemaClass();
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
        if (origin instanceof YTRID) {
          origin = ((YTRID) origin).getRecord();
        }
        if (origin instanceof YTEntity) {
          ((YTEntity) origin).setProperty(iFieldName, iPropertyValue);
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
        if (origin instanceof YTRID) {
          origin = ((YTRID) origin).getRecord();
        }
        if (origin instanceof YTEntityInternal) {
          ((YTEntityInternal) origin).setPropertyInternal(name, value);
        }
      }
    }
  }
}
