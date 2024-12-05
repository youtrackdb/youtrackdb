package com.orientechnologies.core.db.tool.importer;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;

/**
 *
 */
public abstract class OAbstractCollectionConverter<T> implements OValuesConverter<T> {

  private final OConverterData converterData;

  protected OAbstractCollectionConverter(OConverterData converterData) {
    this.converterData = converterData;
  }

  public interface ResultCallback {

    void add(Object item);
  }

  protected boolean convertSingleValue(YTDatabaseSessionInternal db, final Object item,
      ResultCallback result, boolean updated) {
    if (item == null) {
      result.add(null);
      return false;
    }

    if (item instanceof YTIdentifiable) {
      final OValuesConverter<YTIdentifiable> converter =
          (OValuesConverter<YTIdentifiable>)
              OImportConvertersFactory.INSTANCE.getConverter(item, converterData);

      final YTIdentifiable newValue = converter.convert(db, (YTIdentifiable) item);

      // this code intentionally uses == instead of equals, in such case we may distinguish rids
      // which already contained in
      // document and RID which is used to indicate broken record
      if (newValue != OImportConvertersFactory.BROKEN_LINK) {
        result.add(newValue);
      }

      if (!newValue.equals(item)) {
        updated = true;
      }
    } else {
      final OValuesConverter valuesConverter =
          OImportConvertersFactory.INSTANCE.getConverter(item, converterData);
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
