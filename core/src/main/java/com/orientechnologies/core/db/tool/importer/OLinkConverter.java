package com.orientechnologies.core.db.tool.importer;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.tool.ODatabaseImport;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.sql.executor.YTResultSet;

/**
 *
 */
public final class OLinkConverter implements OValuesConverter<YTIdentifiable> {

  private final OConverterData converterData;

  public OLinkConverter(OConverterData importer) {
    this.converterData = importer;
  }

  @Override
  public YTIdentifiable convert(YTDatabaseSessionInternal db, YTIdentifiable value) {
    final YTRID rid = value.getIdentity();
    if (!rid.isPersistent()) {
      return value;
    }

    if (converterData.brokenRids.contains(rid)) {
      return OImportConvertersFactory.BROKEN_LINK;
    }

    try (final YTResultSet resultSet =
        converterData.session.query(
            "select value from " + ODatabaseImport.EXPORT_IMPORT_CLASS_NAME + " where key = ?",
            rid.toString())) {
      if (resultSet.hasNext()) {
        return new YTRecordId(resultSet.next().<String>getProperty("value"));
      }
      return value;
    }
  }
}
