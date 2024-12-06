package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TestSerializationCompatibilityRecord extends DbTestBase {

  @Test
  public void testDataNotMatchSchema() {
    SchemaClass klass =
        db.getMetadata()
            .getSchema()
            .createClass("Test", db.getMetadata().getSchema().getClass("V"));
    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    Map<String, RID> map = new HashMap<String, RID>();
    map.put("some", new RecordId(10, 20));
    doc.field("map", map, PropertyType.LINKMAP);
    RID id = db.save(doc).getIdentity();
    db.commit();
    klass.createProperty(db, "map", PropertyType.EMBEDDEDMAP, (PropertyType) null, true);

    db.begin();
    EntityImpl record = db.load(id);
    // Force deserialize + serialize;
    record.setProperty("some", "aa");
    db.save(record);
    db.commit();

    EntityImpl record1 = db.load(id);
    assertEquals(record1.fieldType("map"), PropertyType.LINKMAP);
  }
}
