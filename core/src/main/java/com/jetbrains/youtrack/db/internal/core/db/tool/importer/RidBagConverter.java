package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
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
    final var result = new RidBag(db);
    var updated = false;
    final ResultCallback callback =
        item -> result.add(((Identifiable) item).getIdentity());

    for (Identifiable identifiable : value) {
      updated = convertSingleValue(db, identifiable, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
