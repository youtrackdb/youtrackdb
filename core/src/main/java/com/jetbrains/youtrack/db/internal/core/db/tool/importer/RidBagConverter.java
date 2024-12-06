package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;

/**
 *
 */
public final class RidBagConverter extends AbstractCollectionConverter<RidBag> {

  public RidBagConverter(ConverterData converterData) {
    super(converterData);
  }

  @Override
  public RidBag convert(DatabaseSessionInternal db, RidBag value) {
    final RidBag result = new RidBag(db);
    boolean updated = false;
    final ResultCallback callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add((Identifiable) item);
          }
        };

    for (Identifiable identifiable : value) {
      updated = convertSingleValue(db, identifiable, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
