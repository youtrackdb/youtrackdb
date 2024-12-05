package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTView;
import com.jetbrains.youtrack.db.internal.core.record.Entity;

public class ViewEntityImpl extends EntityImpl {

  private final YTView view;

  public ViewEntityImpl(YTDatabaseSessionInternal database, int cluster) {
    view = database.getViewFromCluster(cluster);
  }

  public ViewEntityImpl(YTDatabaseSessionInternal database, YTRID rid) {
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
        if (origin instanceof Entity) {
          ((Entity) origin).setProperty(iFieldName, iPropertyValue);
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
        if (origin instanceof EntityInternal) {
          ((EntityInternal) origin).setPropertyInternal(name, value);
        }
      }
    }
  }
}
