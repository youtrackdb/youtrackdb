package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;

/**
 *
 */
public final class ORidBagConverter extends OAbstractCollectionConverter<ORidBag> {

  public ORidBagConverter(OConverterData converterData) {
    super(converterData);
  }

  @Override
  public ORidBag convert(YTDatabaseSessionInternal db, ORidBag value) {
    final ORidBag result = new ORidBag(db);
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
