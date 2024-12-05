package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.ODocumentFieldVisitor;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;

/**
 *
 */
public final class OLinksRewriter implements ODocumentFieldVisitor {

  private final OConverterData converterData;

  public OLinksRewriter(OConverterData converterData) {
    this.converterData = converterData;
  }

  @Override
  public Object visitField(YTDatabaseSessionInternal db, YTType type, YTType linkedType,
      Object value) {
    boolean oldAutoConvertValue = false;

    final OValuesConverter valuesConverter =
        OImportConvertersFactory.INSTANCE.getConverter(value, converterData);
    if (valuesConverter == null) {
      return value;
    }

    final Object newValue = valuesConverter.convert(db, value);

    // this code intentionally uses == instead of equals, in such case we may distinguish rids which
    // already contained in
    // document and RID which is used to indicate broken record
    if (newValue == OImportConvertersFactory.BROKEN_LINK) {
      return null;
    }

    return newValue;
  }

  @Override
  public boolean goFurther(YTType type, YTType linkedType, Object value, Object newValue) {
    return true;
  }

  @Override
  public boolean goDeeper(YTType type, YTType linkedType, Object value) {
    return true;
  }

  @Override
  public boolean updateMode() {
    return true;
  }
}
