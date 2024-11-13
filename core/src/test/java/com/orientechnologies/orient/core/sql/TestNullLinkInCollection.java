package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertNull;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tglman on 02/12/15.
 */
public class TestNullLinkInCollection extends BaseMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();
    db.getMetadata().getSchema().createClass("Test");
  }

  @Test
  public void testLinkListRemovedRecord() {

    db.begin();
    ODocument doc = new ODocument("Test");
    List<ORecordId> docs = new ArrayList<ORecordId>();
    docs.add(new ORecordId(10, 20));
    doc.field("items", docs, OType.LINKLIST);
    db.save(doc);
    db.commit();

    try (OResultSet res = db.query("select items from Test")) {
      assertNull(((List) res.next().getProperty("items")).get(0));
    }
  }

  @Test
  public void testLinkSetRemovedRecord() {
    db.begin();
    ODocument doc = new ODocument("Test");
    Set<ORecordId> docs = new HashSet<ORecordId>();
    docs.add(new ORecordId(10, 20));
    doc.field("items", docs, OType.LINKSET);
    db.save(doc);
    db.commit();

    try (OResultSet res = db.query("select items from Test")) {
      Assert.assertEquals(
          new ORecordId(10, 20), ((Set) res.next().getProperty("items")).iterator().next());
    }
  }
}
