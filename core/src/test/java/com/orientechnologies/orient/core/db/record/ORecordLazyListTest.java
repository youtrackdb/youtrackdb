package com.orientechnologies.orient.core.db.record;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ORecordLazyListTest {

  private OxygenDB oxygenDb;
  private ODatabaseSession dbSession;

  @Before
  public void init() throws Exception {
    oxygenDb =
        OCreateDatabaseUtil.createDatabase(
            ORecordLazyListTest.class.getSimpleName(), "memory:", OCreateDatabaseUtil.TYPE_MEMORY);
    dbSession =
        oxygenDb.open(
            ORecordLazyListTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    OSchema schema = dbSession.getMetadata().getSchema();
    OClass mainClass = schema.createClass("MainClass");
    mainClass.createProperty(dbSession, "name", OType.STRING);
    OProperty itemsProp = mainClass.createProperty(dbSession, "items", OType.LINKLIST);
    OClass itemClass = schema.createClass("ItemClass");
    itemClass.createProperty(dbSession, "name", OType.STRING);
    itemsProp.setLinkedClass(dbSession, itemClass);

    dbSession.begin();
    ODocument doc1 = new ODocument(itemClass).field("name", "Doc1");
    doc1.save();
    ODocument doc2 = new ODocument(itemClass).field("name", "Doc2");
    doc2.save();
    ODocument doc3 = new ODocument(itemClass).field("name", "Doc3");
    doc3.save();

    ODocument mainDoc = new ODocument(mainClass).field("name", "Main Doc");
    mainDoc.field("items", Arrays.asList(doc1, doc2, doc3));
    mainDoc.save();
    dbSession.commit();

    dbSession.begin();

    mainDoc = dbSession.bindToSession(mainDoc);
    Collection<ODocument> origItems = mainDoc.field("items");
    Iterator<ODocument> it = origItems.iterator();
    assertNotNull(it.next());
    assertNotNull(it.next());

    List<ODocument> items = new ArrayList<ODocument>(origItems);
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
    if (oxygenDb != null && dbSession != null) {
      oxygenDb.drop(ORecordLazyListTest.class.getSimpleName());
    }
  }
}
