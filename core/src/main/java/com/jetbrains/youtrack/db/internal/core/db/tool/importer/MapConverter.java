package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 */
public final class MapConverter extends AbstractCollectionConverter<Map> {

  public MapConverter(ConverterData converterData) {
    super(converterData);
  }

  @Override
  public Map convert(DatabaseSessionInternal db, Map value) {
    final Map result = new LinkedHashMap();
    boolean updated = false;
    final class MapResultCallback implements ResultCallback {

      private Object key;

      @Override
      public void add(Object item) {
        result.put(key, item);
      }

      public void setKey(Object key) {
        this.key = key;
      }
    }

    final MapResultCallback callback = new MapResultCallback();
    for (Map.Entry entry : (Iterable<Map.Entry>) value.entrySet()) {
      callback.setKey(entry.getKey());
      updated = convertSingleValue(db, entry.getValue(), callback, updated) || updated;
    }
    if (updated) {
      return result;
    }

    return value;
  }
}
