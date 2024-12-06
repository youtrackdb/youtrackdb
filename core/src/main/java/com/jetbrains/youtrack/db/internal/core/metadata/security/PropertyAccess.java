package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Set;

public class PropertyAccess {

  private final Set<String> filtered;

  public PropertyAccess(DatabaseSessionInternal session, EntityImpl document,
      SecurityInternal security) {
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
