package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import org.junit.Assert;
import org.junit.Test;

public class DefaultValueSerializationTest extends DBTestBase {

  @Test
  public void testKeepValueSerialization() {
    // create example schema
    YTSchema schema = db.getMetadata().getSchema();
    YTClass classA = schema.createClass("ClassC");

    YTProperty prop = classA.createProperty(db, "name", YTType.STRING);
    prop.setDefaultValue(db, "uuid()");

    EntityImpl doc = new EntityImpl("ClassC");

    byte[] val = doc.toStream();
    EntityImpl doc1 = new EntityImpl();
    ORecordInternal.unsetDirty(doc1);
    doc1.fromStream(val);
    doc1.deserializeFields();
    Assert.assertEquals(doc.field("name").toString(), doc1.field("name").toString());
  }
}
