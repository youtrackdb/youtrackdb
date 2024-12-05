package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import java.util.Set;
import javax.annotation.Nullable;

public interface EntityInternal extends Entity {

  Set<String> getPropertyNamesInternal();

  void setPropertyInternal(String name, Object value);

  void setPropertyInternal(String name, Object value, YTType... type);

  <RET> RET removePropertyInternal(String name);

  <RET> RET getPropertyInternal(String name);

  <RET> RET getPropertyInternal(String name, boolean lazyLoading);

  @Nullable
  YTIdentifiable getLinkPropertyInternal(String name);
}
