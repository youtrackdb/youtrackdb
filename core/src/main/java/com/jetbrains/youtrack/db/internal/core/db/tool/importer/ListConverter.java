package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public final class ListConverter extends AbstractCollectionConverter<List> {

  public ListConverter(ConverterData converterData) {
    super(converterData);
  }

  @Override
  public List convert(DatabaseSessionInternal db, List value) {
    final List result = new ArrayList();

    final var callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add(item);
          }
        };
    var updated = false;

    for (var item : value) {
      updated = convertSingleValue(db, item, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
