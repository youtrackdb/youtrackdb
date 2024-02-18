package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import java.util.Set;
import javax.annotation.Nullable;

public interface OElementInternal extends OElement {
  Set<String> getPropertyNamesWithoutFiltration();

  void setPropertyWithoutValidation(String name, Object value);

  void setPropertyWithoutValidation(String name, Object value, OType... type);

  <RET> RET removePropertyWithoutValidation(String name);

  <RET> RET getPropertyWithoutValidation(String name);

  @Nullable
  OIdentifiable getLinkPropertyWithoutValidation(String name);
}
