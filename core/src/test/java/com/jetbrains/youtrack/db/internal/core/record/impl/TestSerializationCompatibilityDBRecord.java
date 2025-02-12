package com.jetbrains.youtrack.db.internal.core.record.impl;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class TestSerializationCompatibilityDBRecord extends DbTestBase {

  @Test
  public void testDataNotMatchSchema() {
    var klass =
        (SchemaClassInternal) session.getMetadata()
            .getSchema()
            .createClass("Test", session.getMetadata().getSchema().getClass("V"));
    session.begin();
    var doc = (EntityImpl) session.newEntity("Test");
    Map<String, RID> map = new HashMap<String, RID>();
    map.put("some", new RecordId(10, 20));
    doc.field("map", map, PropertyType.LINKMAP);
    var id = session.save(doc).getIdentity();
    session.commit();
    klass.createProperty(session, "map", PropertyType.EMBEDDEDMAP,
        (PropertyType) null, true);

    session.begin();
    EntityImpl record = session.load(id);
    // Force deserialize + serialize;
    record.setProperty("some", "aa");
    session.save(record);
    session.commit();

    EntityImpl record1 = session.load(id);
    assertEquals(PropertyType.LINKMAP, record1.getPropertyType("map"));
  }
}
