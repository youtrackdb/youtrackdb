package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentHelper;

/**
 * This class is designed to compare documents based on deep equality (to be used in Sets)
 */
public class ODocumentEqualityWrapper {

  private final EntityImpl internal;

  ODocumentEqualityWrapper(EntityImpl internal) {

    this.internal = internal;
  }

  public boolean equals(Object obj) {
    if (obj instanceof ODocumentEqualityWrapper) {
      return ODocumentHelper.hasSameContentOf(
          internal, internal.getSession(), ((ODocumentEqualityWrapper) obj).internal,
          internal.getSession(), null);
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 0;
    for (String fieldName : internal.fieldNames()) {
      result += fieldName.hashCode();
      Object value = internal.field(fieldName);
      if (value != null) {
        result += value.hashCode();
      }
    }
    return result;
  }
}
