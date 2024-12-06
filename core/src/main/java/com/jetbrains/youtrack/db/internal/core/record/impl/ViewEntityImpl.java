package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaView;
import com.jetbrains.youtrack.db.internal.core.record.Entity;

public class ViewEntityImpl extends EntityImpl {

  private final SchemaView view;

  public ViewEntityImpl(DatabaseSessionInternal database, int cluster) {
    view = database.getViewFromCluster(cluster);
  }

  public ViewEntityImpl(DatabaseSessionInternal database, RID rid) {
    super(database, rid);
    view = database.getViewFromCluster(rid.getClusterId());
  }

  @Override
  public SchemaView getSchemaClass() {
    checkForBinding();

    return view;
  }

  @Override
  protected SchemaImmutableClass getImmutableSchemaClass() {
    checkForBinding();

    if (view instanceof SchemaImmutableClass) {
      return (SchemaImmutableClass) view;
    } else {
      return (SchemaImmutableClass) getImmutableSchema().getView(view.getName());
    }
  }

  @Override
  public String getClassName() {
    SchemaView clazz = getSchemaClass();
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
        if (origin instanceof RID) {
          origin = ((RID) origin).getRecord();
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
        if (origin instanceof RID) {
          origin = ((RID) origin).getRecord();
        }
        if (origin instanceof EntityInternal) {
          ((EntityInternal) origin).setPropertyInternal(name, value);
        }
      }
    }
  }
}
