package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import java.util.Set;
import javax.annotation.Nullable;

public interface YTEntityInternal extends YTEntity {

  Set<String> getPropertyNamesInternal();

  void setPropertyInternal(String name, Object value);

  void setPropertyInternal(String name, Object value, YTType... type);

  <RET> RET removePropertyInternal(String name);

  <RET> RET getPropertyInternal(String name);

  <RET> RET getPropertyInternal(String name, boolean lazyLoading);

  @Nullable
  YTIdentifiable getLinkPropertyInternal(String name);
}
