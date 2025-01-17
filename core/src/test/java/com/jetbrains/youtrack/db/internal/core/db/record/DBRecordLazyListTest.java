package com.jetbrains.youtrack.db.internal.core.db.record;

import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DBRecordLazyListTest {

  private YouTrackDB youTrackDb;
  private DatabaseSessionInternal dbSession;

  @Before
  public void init() throws Exception {
    youTrackDb =
        CreateDatabaseUtil.createDatabase(
            DBRecordLazyListTest.class.getSimpleName(), "memory:", CreateDatabaseUtil.TYPE_MEMORY);
    dbSession =
        (DatabaseSessionInternal) youTrackDb.open(
            DBRecordLazyListTest.class.getSimpleName(),
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    Schema schema = dbSession.getMetadata().getSchema();
    SchemaClass mainClass = schema.createClass("MainClass");
    mainClass.createProperty(dbSession, "name", PropertyType.STRING);
    SchemaProperty itemsProp = mainClass.createProperty(dbSession, "items", PropertyType.LINKLIST);
    SchemaClass itemClass = schema.createClass("ItemClass");
    itemClass.createProperty(dbSession, "name", PropertyType.STRING);
    itemsProp.setLinkedClass(dbSession, itemClass);

    dbSession.begin();
    EntityImpl doc1 = new EntityImpl(itemClass).field("name", "Doc1");
    doc1.save();
    EntityImpl doc2 = new EntityImpl(itemClass).field("name", "Doc2");
    doc2.save();
    EntityImpl doc3 = new EntityImpl(itemClass).field("name", "Doc3");
    doc3.save();

    EntityImpl mainDoc = new EntityImpl(mainClass).field("name", "Main Doc");
    mainDoc.field("items", Arrays.asList(doc1, doc2, doc3));
    mainDoc.save();
    dbSession.commit();

    dbSession.begin();

    mainDoc = dbSession.bindToSession(mainDoc);
    Collection<EntityImpl> origItems = mainDoc.field("items");
    Iterator<EntityImpl> it = origItems.iterator();
    assertNotNull(it.next());
    assertNotNull(it.next());

    List<EntityImpl> items = new ArrayList<EntityImpl>(origItems);
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
      youTrackDb.drop(DBRecordLazyListTest.class.getSimpleName());
    }
  }
}
