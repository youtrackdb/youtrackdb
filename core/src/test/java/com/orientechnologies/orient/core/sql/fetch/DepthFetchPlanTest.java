package com.orientechnologies.orient.core.sql.fetch;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.fetch.OFetchContext;
import com.orientechnologies.orient.core.fetch.OFetchHelper;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchContext;
import com.orientechnologies.orient.core.fetch.remote.ORemoteFetchListener;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import org.junit.Test;

public class DepthFetchPlanTest extends DBTestBase {

  @Test
  public void testFetchPlanDepth() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
    YTEntityImpl doc1 = new YTEntityImpl("Test");
    YTEntityImpl doc2 = new YTEntityImpl("Test");
    doc.field("name", "name");
    db.save(doc);
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    doc1.field("name", "name1");
    doc1.field("ref", doc);
    db.save(doc1);
    db.commit();

    db.begin();
    doc1 = db.bindToSession(doc1);
    doc2.field("name", "name2");
    doc2.field("ref", doc1);
    db.save(doc2);
    db.commit();

    doc2 = db.bindToSession(doc2);
    OFetchContext context = new ORemoteFetchContext();
    CountFetchListener listener = new CountFetchListener();
    OFetchHelper.fetch(
        doc2, doc2, OFetchHelper.buildFetchPlan("ref:1 *:-2"), listener, context, "");

    assertEquals(1, listener.count);
  }

  @Test
  public void testFullDepthFetchPlan() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
    YTEntityImpl doc1 = new YTEntityImpl("Test");
    YTEntityImpl doc2 = new YTEntityImpl("Test");
    YTEntityImpl doc3 = new YTEntityImpl("Test");
    doc.field("name", "name");
    db.save(doc);
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    doc1.field("name", "name1");
    doc1.field("ref", doc);
    db.save(doc1);
    db.commit();

    db.begin();
    doc1 = db.bindToSession(doc1);

    doc2.field("name", "name2");
    doc2.field("ref", doc1);
    db.save(doc2);
    db.commit();

    db.begin();
    doc2 = db.bindToSession(doc2);

    doc3.field("name", "name2");
    doc3.field("ref", doc2);
    db.save(doc3);
    db.commit();

    doc3 = db.bindToSession(doc3);
    OFetchContext context = new ORemoteFetchContext();
    CountFetchListener listener = new CountFetchListener();
    OFetchHelper.fetch(doc3, doc3, OFetchHelper.buildFetchPlan("[*]ref:-1"), listener, context, "");
    assertEquals(3, listener.count);
  }

  private final class CountFetchListener extends ORemoteFetchListener {

    public int count;

    @Override
    public boolean requireFieldProcessing() {
      return true;
    }

    @Override
    protected void sendRecord(YTRecordAbstract iLinked) {
      count++;
    }
  }
}
