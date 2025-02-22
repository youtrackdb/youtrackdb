package com.jetbrains.youtrack.db.internal.core.db.record;

import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DBRecordLazyListTest {

  private YouTrackDB youTrackDb;
  private DatabaseSessionInternal db;

  @Before
  public void init() throws Exception {
    youTrackDb =
        CreateDatabaseUtil.createDatabase(
            DBRecordLazyListTest.class.getSimpleName(), "memory:", CreateDatabaseUtil.TYPE_MEMORY);
    db =
        (DatabaseSessionInternal) youTrackDb.open(
            DBRecordLazyListTest.class.getSimpleName(),
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    var schema = db.getMetadata().getSchema();
    var mainClass = schema.createClass("MainClass");
    mainClass.createProperty(db, "name", PropertyType.STRING);
    var itemsProp = mainClass.createProperty(db, "items", PropertyType.LINKLIST);
    var itemClass = schema.createClass("ItemClass");
    itemClass.createProperty(db, "name", PropertyType.STRING);
    itemsProp.setLinkedClass(db, itemClass);

    db.begin();
    var doc1 = ((EntityImpl) db.newEntity(itemClass)).field("name", "Doc1");

    var doc2 = ((EntityImpl) db.newEntity(itemClass)).field("name", "Doc2");

    var doc3 = ((EntityImpl) db.newEntity(itemClass)).field("name", "Doc3");

    var mainDoc = ((EntityImpl) db.newEntity(mainClass)).field("name", "Main Doc");
    mainDoc.newLinkList("items").addAll(Arrays.asList(doc1, doc2, doc3));
    db.commit();

    db.begin();
    mainDoc = db.bindToSession(mainDoc);
    Collection<EntityImpl> origItems = mainDoc.field("items");
    var it = origItems.iterator();
    assertNotNull(it.next());
    assertNotNull(it.next());

    List<EntityImpl> items = new ArrayList<EntityImpl>(origItems);
    assertNotNull(items.get(0));
    assertNotNull(items.get(1));
    assertNotNull(items.get(2));
    db.rollback();
  }

  @After
  public void close() {
    if (db != null) {
      db.close();
    }
    if (youTrackDb != null && db != null) {
      youTrackDb.drop(DBRecordLazyListTest.class.getSimpleName());
    }
  }
}
