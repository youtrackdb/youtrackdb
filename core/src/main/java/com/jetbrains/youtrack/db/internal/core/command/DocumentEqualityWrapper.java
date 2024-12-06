package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentHelper;

/**
 * This class is designed to compare documents based on deep equality (to be used in Sets)
 */
public class DocumentEqualityWrapper {

  private final EntityImpl internal;

  DocumentEqualityWrapper(EntityImpl internal) {

    this.internal = internal;
  }

  public boolean equals(Object obj) {
    if (obj instanceof DocumentEqualityWrapper) {
      return DocumentHelper.hasSameContentOf(
          internal, internal.getSession(), ((DocumentEqualityWrapper) obj).internal,
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
