package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.tool.ODatabaseImport;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;

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
