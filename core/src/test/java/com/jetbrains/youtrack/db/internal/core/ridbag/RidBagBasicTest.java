package com.jetbrains.youtrack.db.internal.core.ridbag;

import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.embedded.EmbeddedRidBag;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class RidBagBasicTest extends DbTestBase {
  @Test(expected = IllegalArgumentException.class)
  public void testExceptionInCaseOfNull() {
    var bag = new EmbeddedRidBag(session);
    bag.add(null);
  }

  @Test
  public void allowOnlyAtRoot() {
    try {
      var record = session.newVertex();
      List<Object> valueList = new ArrayList<>();
      valueList.add(new RidBag(session));
      record.setProperty("emb", valueList);
      session.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (DatabaseException ex) {
      // this is expected
    }

    try {
      var record = session.newVertex();
      Set<Object> valueSet = new HashSet<>();
      valueSet.add(new RidBag(session));
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (DatabaseException ex) {
      // this is expected
    }

    try {
      var record = session.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      valueSet.put("key", new RidBag(session));
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (DatabaseException ex) {
      // this is expected
    }

    try {
      var record = session.newVertex();
      Map<String, Object> valueSet = new HashMap<>();
      var nested = session.newEntity();
      nested.setProperty("bag", new RidBag(session));
      valueSet.put("key", nested);
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (DatabaseException ex) {
      // this is expected
    }

    try {
      var record = session.newVertex();
      List<Object> valueList = new ArrayList<>();
      var nested = session.newEntity();
      nested.setProperty("bag", new RidBag(session));
      valueList.add(nested);
      record.setProperty("emb", valueList);
      session.save(record);
      fail("Should not be possible to save a ridbag in a list");
    } catch (DatabaseException ex) {
      // this is expected
    }

    try {
      var record = session.newVertex();
      Set<Object> valueSet = new HashSet<>();
      var nested = session.newEntity();
      nested.setProperty("bag", new RidBag(session));
      valueSet.add(nested);
      record.setProperty("emb", valueSet);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (DatabaseException ex) {
      // this is expected
    }

    try {
      var record = session.newVertex();
      var nested = session.newEntity();
      nested.setProperty("bag", new RidBag(session));
      record.setProperty("emb", nested);
      session.save(record);
      fail("Should not be possible to save a ridbag in a set");
    } catch (DatabaseException ex) {
      // this is expected
    }
  }
}
