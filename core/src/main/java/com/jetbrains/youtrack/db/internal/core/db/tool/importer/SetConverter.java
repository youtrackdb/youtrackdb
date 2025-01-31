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
    var updated = false;
    final Set result;

    result = new HashSet();

    final var callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add(item);
          }
        };

    for (var item : value) {
      updated = convertSingleValue(db, item, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
