package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ORecordInternal;
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

    YTEntityImpl doc = new YTEntityImpl("ClassC");

    byte[] val = doc.toStream();
    YTEntityImpl doc1 = new YTEntityImpl();
    ORecordInternal.unsetDirty(doc1);
    doc1.fromStream(val);
    doc1.deserializeFields();
    Assert.assertEquals(doc.field("name").toString(), doc1.field("name").toString());
  }
}
