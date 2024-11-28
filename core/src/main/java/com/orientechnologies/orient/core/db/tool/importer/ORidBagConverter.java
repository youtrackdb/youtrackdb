package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;

/**
 *
 */
public final class ORidBagConverter extends OAbstractCollectionConverter<ORidBag> {

  public ORidBagConverter(OConverterData converterData) {
    super(converterData);
  }

  @Override
  public ORidBag convert(ODatabaseSessionInternal db, ORidBag value) {
    final ORidBag result = new ORidBag(db);
    boolean updated = false;
    final ResultCallback callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add((OIdentifiable) item);
          }
        };

    for (OIdentifiable identifiable : value) {
      updated = convertSingleValue(db, identifiable, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
