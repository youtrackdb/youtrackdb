package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TestSerializationCompatibilityRecord extends DBTestBase {

  @Test
  public void testDataNotMatchSchema() {
    YTClass klass =
        db.getMetadata()
            .getSchema()
            .createClass("Test", db.getMetadata().getSchema().getClass("V"));
    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    Map<String, YTRID> map = new HashMap<String, YTRID>();
    map.put("some", new YTRecordId(10, 20));
    doc.field("map", map, YTType.LINKMAP);
    YTRID id = db.save(doc).getIdentity();
    db.commit();
    klass.createProperty(db, "map", YTType.EMBEDDEDMAP, (YTType) null, true);

    db.begin();
    EntityImpl record = db.load(id);
    // Force deserialize + serialize;
    record.setProperty("some", "aa");
    db.save(record);
    db.commit();

    EntityImpl record1 = db.load(id);
    assertEquals(record1.fieldType("map"), YTType.LINKMAP);
  }
}
