package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public final class ImportConvertersFactory {

  public static final RID BROKEN_LINK = new RecordId(-1, -42);
  public static final ImportConvertersFactory INSTANCE = new ImportConvertersFactory();

  public ValuesConverter getConverter(Object value, ConverterData converterData) {
    if (value instanceof Map) {
      return new MapConverter(converterData);
    }

    if (value instanceof List) {
      return new ListConverter(converterData);
    }

    if (value instanceof Set) {
      return new SetConverter(converterData);
    }

    if (value instanceof RidBag) {
      return new RidBagConverter(converterData);
    }

    if (value instanceof Identifiable) {
      return new LinkConverter(converterData);
    }

    return null;
  }
}
