package com.orientechnologies.orient.core.ridbag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.db.record.ridbag.embedded.OEmbeddedRidBag;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;

public class ORidBagBasicTest extends DBTestBase {

  @Test
  public void embeddedRidBagSerializationTest() {
    OEmbeddedRidBag bag = new OEmbeddedRidBag();

    bag.add(new YTRecordId(3, 1000));
    bag.convertRecords2Links();
    byte[] bytes = new byte[1024];
    UUID id = UUID.randomUUID();
    bag.serialize(bytes, 0, id);

    OEmbeddedRidBag bag1 = new OEmbeddedRidBag();
    bag1.deserialize(bytes, 0);

    assertEquals(bag.size(), 1);

    assertEquals(new YTRecordId(3, 1000), bag1.iterator().next());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceptionInCaseOfNull() {
    OEmbeddedRidBag bag = new OEmbeddedRidBag();
    bag.add(null);
  }

  @Test
  public void allowOnlyAtRoot() {
    try {
      YTVertex record = db.newVertex();
      List<Object> valueList = new ArrayList<>();
      valueList.add(new ORidBag(db));
      record.setProperty("emb", valueList);
      db.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      YTVertex record = db.newVertex();
      Set<Object> valueSet = new HashSet<>();
      valueSet.add(new ORidBag(db));
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      YTVertex record = db.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      valueSet.put("key", new ORidBag(db));
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      YTVertex record = db.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      YTEntity nested = db.newElement();
      nested.setProperty("bag", new ORidBag(db));
      valueSet.put("key", nested);
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      YTVertex record = db.newVertex();
      List<Object> valueList = new ArrayList<>();
      YTEntity nested = db.newElement();
      nested.setProperty("bag", new ORidBag(db));
      valueList.add(nested);
      record.setProperty("emb", valueList);
      db.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      YTVertex record = db.newVertex();
      Set<Object> valueSet = new HashSet<>();
      YTEntity nested = db.newElement();
      nested.setProperty("bag", new ORidBag(db));
      valueSet.add(nested);
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      YTVertex record = db.newVertex();
      YTEntity nested = db.newElement();
      nested.setProperty("bag", new ORidBag(db));
      record.setProperty("emb", nested);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }
  }
}
