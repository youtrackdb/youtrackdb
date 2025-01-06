package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import java.util.Collection;
import javax.annotation.Nullable;

public interface EntityInternal extends Entity {

  Collection<String> getPropertyNamesInternal();

  void setPropertyInternal(String name, Object value);

  void setPropertyInternal(String name, Object value, PropertyType type);

  <RET> RET removePropertyInternal(String name);

  <RET> RET getPropertyInternal(String name);

  <RET> RET getPropertyInternal(String name, boolean lazyLoading);

  @Nullable
  Identifiable getLinkPropertyInternal(String name);
}
