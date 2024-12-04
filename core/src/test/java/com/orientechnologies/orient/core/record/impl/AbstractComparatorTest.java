package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.BytesContainer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryComparator;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.OBinaryField;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import org.junit.Assert;

public abstract class AbstractComparatorTest extends DBTestBase {

  protected ODocumentSerializer serializer =
      ORecordSerializerBinary.INSTANCE.getCurrentSerializer();
  protected OBinaryComparator comparator = serializer.getComparator();

  protected void testEquals(ODatabaseSessionInternal db, OType sourceType, OType destType) {
    try {
      Assert.assertTrue(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 10)));
      Assert.assertFalse(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 9)));
      Assert.assertFalse(comparator.isEqual(field(db, sourceType, 10), field(db, destType, 11)));
    } catch (AssertionError e) {
      System.out.println("ERROR: testEquals(" + sourceType + "," + destType + ")");
      throw e;
    }
  }

  protected OBinaryField field(ODatabaseSessionInternal db, final OType type, final Object value) {
    return field(db, type, value, null);
  }

  protected OBinaryField field(ODatabaseSessionInternal db, final OType type, final Object value,
      OCollate collate) {
    BytesContainer bytes = new BytesContainer();
    bytes.offset = serializer.serializeValue(db, bytes, value, type, null, null, null);
    return new OBinaryField(null, type, bytes, collate);
  }
}
