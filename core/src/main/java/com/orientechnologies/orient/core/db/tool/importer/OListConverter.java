package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public final class OListConverter extends OAbstractCollectionConverter<List> {

  public OListConverter(OConverterData converterData) {
    super(converterData);
  }

  @Override
  public List convert(ODatabaseSessionInternal db, List value) {
    final List result = new ArrayList();

    final ResultCallback callback =
        new ResultCallback() {
          @Override
          public void add(Object item) {
            result.add(item);
          }
        };
    boolean updated = false;

    for (Object item : value) {
      updated = convertSingleValue(db, item, callback, updated);
    }

    if (updated) {
      return result;
    }

    return value;
  }
}
