package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestNullLinkInCollection extends DbTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    db.getMetadata().getSchema().createClass("Test");
  }

  @Test
  public void testLinkListRemovedRecord() {

    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    List<RecordId> docs = new ArrayList<>();
    docs.add(new RecordId(10, 20));
    doc.field("items", docs, PropertyType.LINKLIST);
    db.save(doc);
    db.commit();

    try (ResultSet res = db.query("select items from Test")) {
      assertEquals(new RecordId(10, 20), ((List) res.next().getProperty("items")).get(0));
    }
  }

  @Test
  public void testLinkSetRemovedRecord() {
    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    Set<RecordId> docs = new HashSet<>();
    docs.add(new RecordId(10, 20));
    doc.field("items", docs, PropertyType.LINKSET);
    db.save(doc);
    db.commit();

    try (ResultSet res = db.query("select items from Test")) {
      Assert.assertEquals(
          new RecordId(10, 20), ((Set) res.next().getProperty("items")).iterator().next());
    }
  }
}
