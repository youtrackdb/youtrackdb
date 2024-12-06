package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public final class SetConverter extends AbstractCollectionConverter<Set> {

  public SetConverter(ConverterData converterData) {
    super(converterData);
  }

  @Override
  public Set convert(DatabaseSessionInternal db, Set value) {
    boolean updated = false;
    final Set result;

    result = new HashSet();

    final ResultCallback callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add(item);
          }
        };

    for (Object item : value) {
      updated = convertSingleValue(db, item, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
