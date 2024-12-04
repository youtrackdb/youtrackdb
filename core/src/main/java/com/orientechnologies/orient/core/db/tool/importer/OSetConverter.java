package com.orientechnologies.orient.core.db.tool.importer;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import java.util.HashSet;
import java.util.Set;

/**
 *
 */
public final class OSetConverter extends OAbstractCollectionConverter<Set> {

  public OSetConverter(OConverterData converterData) {
    super(converterData);
  }

  @Override
  public Set convert(YTDatabaseSessionInternal db, Set value) {
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
