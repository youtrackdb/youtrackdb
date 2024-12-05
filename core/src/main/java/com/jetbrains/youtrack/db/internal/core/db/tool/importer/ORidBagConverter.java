package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;

/**
 *
 */
public final class ORidBagConverter extends OAbstractCollectionConverter<RidBag> {

  public ORidBagConverter(OConverterData converterData) {
    super(converterData);
  }

  @Override
  public RidBag convert(YTDatabaseSessionInternal db, RidBag value) {
    final RidBag result = new RidBag(db);
    boolean updated = false;
    final ResultCallback callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add((YTIdentifiable) item);
          }
        };

    for (YTIdentifiable identifiable : value) {
      updated = convertSingleValue(db, identifiable, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
