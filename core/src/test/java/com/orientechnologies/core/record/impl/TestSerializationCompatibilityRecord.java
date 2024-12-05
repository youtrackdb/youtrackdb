package com.orientechnologies.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
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
    YTEntityImpl doc = new YTEntityImpl("Test");
    Map<String, YTRID> map = new HashMap<String, YTRID>();
    map.put("some", new YTRecordId(10, 20));
    doc.field("map", map, YTType.LINKMAP);
    YTRID id = db.save(doc).getIdentity();
    db.commit();
    klass.createProperty(db, "map", YTType.EMBEDDEDMAP, (YTType) null, true);

    db.begin();
    YTEntityImpl record = db.load(id);
    // Force deserialize + serialize;
    record.setProperty("some", "aa");
    db.save(record);
    db.commit();

    YTEntityImpl record1 = db.load(id);
    assertEquals(record1.fieldType("map"), YTType.LINKMAP);
  }
}
