package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.collate.OCollate;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.BytesContainer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.OBinaryComparator;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.OBinaryField;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import org.junit.Assert;

public abstract class AbstractComparatorTest extends DBTestBase {

  protected ODocumentSerializer serializer =
      ORecordSerializerBinary.INSTANCE.getCurrentSerializer();
  protected OBinaryComparator comparator = serializer.getComparator();

  protected void testEquals(YTDatabaseSessionInternal db, YTType sourceType, YTType destType) {
    try {
      Assert.assertTrue(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 10)));
      Assert.assertFalse(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 9)));
      Assert.assertFalse(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 11)));
    } catch (AssertionError e) {
      System.out.println("ERROR: testEquals(" + sourceType + "," + destType + ")");
      throw e;
    }
  }

  protected OBinaryField field(YTDatabaseSessionInternal db, final YTType type,
      final Object value) {
    return field(db, type, value, null);
  }

  protected OBinaryField field(YTDatabaseSessionInternal db, final YTType type, final Object value,
      OCollate collate) {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = serializer.serializeValue(db, bytes, value, type, null, null, null);
    return new OBinaryField(null, type, bytes, collate);
  }
}
