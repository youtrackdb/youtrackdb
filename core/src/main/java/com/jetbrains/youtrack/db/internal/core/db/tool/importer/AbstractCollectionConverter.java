package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;

/**
 *
 */
public abstract class AbstractCollectionConverter<T> implements ValuesConverter<T> {

  private final ConverterData converterData;

  protected AbstractCollectionConverter(ConverterData converterData) {
    this.converterData = converterData;
  }

  public interface ResultCallback {

    void add(Object item);
  }

  protected boolean convertSingleValue(DatabaseSessionInternal db, final Object item,
      ResultCallback result, boolean updated) {
    if (item == null) {
      result.add(null);
      return false;
    }

    if (item instanceof Identifiable) {
      final ValuesConverter<Identifiable> converter =
          (ValuesConverter<Identifiable>)
              ImportConvertersFactory.INSTANCE.getConverter(item, converterData);

      final Identifiable newValue = converter.convert(db, (Identifiable) item);

      // this code intentionally uses == instead of equals, in such case we may distinguish rids
      // which already contained in
      // document and RID which is used to indicate broken record
      if (newValue != ImportConvertersFactory.BROKEN_LINK) {
        result.add(newValue);
      }

      if (!newValue.equals(item)) {
        updated = true;
      }
    } else {
      final ValuesConverter valuesConverter =
          ImportConvertersFactory.INSTANCE.getConverter(item, converterData);
      if (valuesConverter == null) {
        result.add(item);
      } else {
        final Object newValue = valuesConverter.convert(db, item);
        if (newValue != item) {
          updated = true;
        }

        result.add(newValue);
      }
    }

    return updated;
  }
}
