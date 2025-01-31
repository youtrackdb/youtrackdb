package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import org.junit.Assert;
import org.junit.Test;

public class DefaultValueSerializationTest extends DbTestBase {

  @Test
  public void testKeepValueSerialization() {
    // create example schema
    Schema schema = db.getMetadata().getSchema();
    var classA = schema.createClass("ClassC");

    var prop = classA.createProperty(db, "name", PropertyType.STRING);
    prop.setDefaultValue(db, "uuid()");

    var doc = (EntityImpl) db.newEntity("ClassC");

    var val = doc.toStream();
    var doc1 = (EntityImpl) db.newEntity();
    RecordInternal.unsetDirty(doc1);
    doc1.fromStream(val);
    doc1.deserializeFields();
    Assert.assertEquals(doc.field("name").toString(), doc1.field("name").toString());
  }
}
