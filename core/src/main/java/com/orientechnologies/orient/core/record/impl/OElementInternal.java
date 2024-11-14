package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Set;
import javax.annotation.Nullable;

public interface OElementInternal extends OElement {
  Set<String> getPropertyNamesInternal();

  void setPropertyInternal(String name, Object value);

  void setPropertyInternal(String name, Object value, OType... type);

  <RET> RET removePropertyInternal(String name);

  <RET> RET getPropertyInternal(String name);

  <RET> RET getPropertyInternal(String name, boolean lazyLoading);

  @Nullable
  OIdentifiable getLinkPropertyInternal(String name);
}
