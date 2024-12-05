package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class TestNullLinkInCollection extends DBTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    db.getMetadata().getSchema().createClass("Test");
  }

  @Test
  public void testLinkListRemovedRecord() {

    db.begin();
    YTDocument doc = new YTDocument("Test");
    List<YTRecordId> docs = new ArrayList<>();
    docs.add(new YTRecordId(10, 20));
    doc.field("items", docs, YTType.LINKLIST);
    db.save(doc);
    db.commit();

    try (YTResultSet res = db.query("select items from Test")) {
      assertEquals(new YTRecordId(10, 20), ((List) res.next().getProperty("items")).get(0));
    }
  }

  @Test
  public void testLinkSetRemovedRecord() {
    db.begin();
    YTDocument doc = new YTDocument("Test");
    Set<YTRecordId> docs = new HashSet<>();
    docs.add(new YTRecordId(10, 20));
    doc.field("items", docs, YTType.LINKSET);
    db.save(doc);
    db.commit();

    try (YTResultSet res = db.query("select items from Test")) {
      Assert.assertEquals(
          new YTRecordId(10, 20), ((Set) res.next().getProperty("items")).iterator().next());
    }
  }
}
