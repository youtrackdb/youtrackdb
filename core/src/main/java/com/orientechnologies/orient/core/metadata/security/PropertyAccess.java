package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.Set;

public class PropertyAccess {

  private final Set<String> filtered;

  public PropertyAccess(YTDatabaseSessionInternal session, YTEntityImpl document,
      OSecurityInternal security) {
    filtered = security.getFilteredProperties(session, document);
  }

  public PropertyAccess(Set<String> filtered) {
    this.filtered = filtered;
  }

  public boolean hasFilters() {
    return filtered != null && !filtered.isEmpty();
  }

  public boolean isReadable(String property) {
    return filtered == null || !filtered.contains(property);
  }
}
