package com.jetbrains.youtrack.db.internal.core.ridbag;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.embedded.EmbeddedRidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
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
    EmbeddedRidBag bag = new EmbeddedRidBag();

    bag.add(new YTRecordId(3, 1000));
    bag.convertRecords2Links();
    byte[] bytes = new byte[1024];
    UUID id = UUID.randomUUID();
    bag.serialize(bytes, 0, id);

    EmbeddedRidBag bag1 = new EmbeddedRidBag();
    bag1.deserialize(bytes, 0);

    assertEquals(bag.size(), 1);

    assertEquals(new YTRecordId(3, 1000), bag1.iterator().next());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testExceptionInCaseOfNull() {
    EmbeddedRidBag bag = new EmbeddedRidBag();
    bag.add(null);
  }

  @Test
  public void allowOnlyAtRoot() {
    try {
      Vertex record = db.newVertex();
      List<Object> valueList = new ArrayList<>();
      valueList.add(new RidBag(db));
      record.setProperty("emb", valueList);
      db.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      Vertex record = db.newVertex();
      Set<Object> valueSet = new HashSet<>();
      valueSet.add(new RidBag(db));
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      Vertex record = db.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      valueSet.put("key", new RidBag(db));
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      Vertex record = db.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      Entity nested = db.newEntity();
      nested.setProperty("bag", new RidBag(db));
      valueSet.put("key", nested);
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      Vertex record = db.newVertex();
      List<Object> valueList = new ArrayList<>();
      Entity nested = db.newEntity();
      nested.setProperty("bag", new RidBag(db));
      valueList.add(nested);
      record.setProperty("emb", valueList);
      db.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      Vertex record = db.newVertex();
      Set<Object> valueSet = new HashSet<>();
      Entity nested = db.newEntity();
      nested.setProperty("bag", new RidBag(db));
      valueSet.add(nested);
      record.setProperty("emb", valueSet);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }

    try {
      Vertex record = db.newVertex();
      Entity nested = db.newEntity();
      nested.setProperty("bag", new RidBag(db));
      record.setProperty("emb", nested);
      db.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (YTDatabaseException ex) {
      // this is expected
    }
  }
}
