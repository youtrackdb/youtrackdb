package com.orientechnologies.core.db.record;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
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
    YTEntityImpl doc1 = new YTEntityImpl(itemClass).field("name", "Doc1");
    doc1.save();
    YTEntityImpl doc2 = new YTEntityImpl(itemClass).field("name", "Doc2");
    doc2.save();
    YTEntityImpl doc3 = new YTEntityImpl(itemClass).field("name", "Doc3");
    doc3.save();

    YTEntityImpl mainDoc = new YTEntityImpl(mainClass).field("name", "Main Doc");
    mainDoc.field("items", Arrays.asList(doc1, doc2, doc3));
    mainDoc.save();
    dbSession.commit();

    dbSession.begin();

    mainDoc = dbSession.bindToSession(mainDoc);
    Collection<YTEntityImpl> origItems = mainDoc.field("items");
    Iterator<YTEntityImpl> it = origItems.iterator();
    assertNotNull(it.next());
    assertNotNull(it.next());

    List<YTEntityImpl> items = new ArrayList<YTEntityImpl>(origItems);
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
