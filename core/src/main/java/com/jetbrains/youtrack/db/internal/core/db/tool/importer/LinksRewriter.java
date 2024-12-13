package com.jetbrains.youtrack.db.internal.core.db.tool.importer;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.EntityPropertiesVisitor;
import com.jetbrains.youtrack.db.api.schema.PropertyType;

/**
 *
 */
public final class LinksRewriter implements EntityPropertiesVisitor {

  private final ConverterData converterData;

  public LinksRewriter(ConverterData converterData) {
    this.converterData = converterData;
  }

  @Override
  public Object visitField(DatabaseSessionInternal db, PropertyType type, PropertyType linkedType,
      Object value) {
    boolean oldAutoConvertValue = false;

    final ValuesConverter valuesConverter =
        ImportConvertersFactory.INSTANCE.getConverter(value, converterData);
    if (valuesConverter == null) {
      return value;
    }

    final Object newValue = valuesConverter.convert(db, value);

    // this code intentionally uses == instead of equals, in such case we may distinguish rids which
    // already contained in
    // document and RID which is used to indicate broken record
    if (newValue == ImportConvertersFactory.BROKEN_LINK) {
      return null;
    }

    return newValue;
  }

  @Override
  public boolean goFurther(PropertyType type, PropertyType linkedType, Object value,
      Object newValue) {
    return true;
  }

  @Override
  public boolean goDeeper(PropertyType type, PropertyType linkedType, Object value) {
    return true;
  }

  @Override
  public boolean updateMode() {
    return true;
  }
}
