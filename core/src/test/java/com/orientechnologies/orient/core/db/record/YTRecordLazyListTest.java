package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class YTRecordLazyListTest {

  private YouTrackDB youTrackDb;
  private YTDatabaseSessionInternal dbSession;

  @Before
  public void init() throws Exception {
    youTrackDb =
        OCreateDatabaseUtil.createDatabase(
            YTRecordLazyListTest.class.getSimpleName(), "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
    dbSession =
        (YTDatabaseSessionInternal) youTrackDb.open(
            YTRecordLazyListTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    YTSchema schema = dbSession.getMetadata().getSchema();
    YTClass mainClass = schema.createClass("MainClass");
    mainClass.createProperty(dbSession, "name", YTType.STRING);
    YTProperty itemsProp = mainClass.createProperty(dbSession, "items", YTType.LINKLIST);
    YTClass itemClass = schema.createClass("ItemClass");
    itemClass.createProperty(dbSession, "name", YTType.STRING);
    itemsProp.setLinkedClass(dbSession, itemClass);

    dbSession.begin();
    YTDocument doc1 = new YTDocument(itemClass).field("name", "Doc1");
    doc1.save();
    YTDocument doc2 = new YTDocument(itemClass).field("name", "Doc2");
    doc2.save();
    YTDocument doc3 = new YTDocument(itemClass).field("name", "Doc3");
    doc3.save();

    YTDocument mainDoc = new YTDocument(mainClass).field("name", "Main Doc");
    mainDoc.field("items", Arrays.asList(doc1, doc2, doc3));
    mainDoc.save();
    dbSession.commit();

    dbSession.begin();

    mainDoc = dbSession.bindToSession(mainDoc);
    Collection<YTDocument> origItems = mainDoc.field("items");
    Iterator<YTDocument> it = origItems.iterator();
    assertNotNull(it.next());
    assertNotNull(it.next());

    List<YTDocument> items = new ArrayList<YTDocument>(origItems);
    assertNotNull(items.get(0));
    assertNotNull(items.get(1));
    assertNotNull(items.get(2));
    dbSession.rollback();
  }

  @After
  public void close() {
    if (dbSession != null) {
      dbSession.close();
    }
    if (youTrackDb != null && dbSession != null) {
      youTrackDb.drop(YTRecordLazyListTest.class.getSimpleName());
    }
  }
}
