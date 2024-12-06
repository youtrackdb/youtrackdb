package com.jetbrains.youtrack.db.internal.core.tx;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class IndexChangesQueryTest {

  public static final String CLASS_NAME = "idxTxAwareMultiValueGetEntriesTest";
  private static final String FIELD_NAME = "value";
  private static final String INDEX_NAME = "idxTxAwareMultiValueGetEntriesTestIndex";
  private YouTrackDB youTrackDB;
  private DatabaseSessionInternal database;

  @Before
  public void before() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase("test", DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
    database =
        (DatabaseSessionInternal)
            youTrackDB.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass cls = schema.createClass(CLASS_NAME);
    cls.createProperty(database, FIELD_NAME, PropertyType.INTEGER);
    cls.createIndex(database, INDEX_NAME, SchemaClass.INDEX_TYPE.NOTUNIQUE, FIELD_NAME);
  }

  @After
  public void after() {
    database.close();
    youTrackDB.close();
  }

  @Test
  public void testMultiplePut() {
    database.begin();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    EntityImpl doc = new EntityImpl(CLASS_NAME);
    doc.field(FIELD_NAME, 1);
    doc.save();

    EntityImpl doc1 = new EntityImpl(CLASS_NAME);
    doc1.field(FIELD_NAME, 2);
    doc1.save();
    Assert.assertNotNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Assert.assertFalse(fetchCollectionFromIndex(index, 1).isEmpty());
    Assert.assertFalse((fetchCollectionFromIndex(index, 2)).isEmpty());

    database.commit();

    Assert.assertEquals(index.getInternal().size(database), 2);
    Assert.assertFalse((fetchCollectionFromIndex(index, 1)).isEmpty());
    Assert.assertFalse((fetchCollectionFromIndex(index, 2)).isEmpty());
  }

  private Collection<RID> fetchCollectionFromIndex(Index index, int key) {
    try (Stream<RID> stream = index.getInternal().getRids(database, key)) {
      return stream.collect(Collectors.toList());
    }
  }

  @Test
  public void testClearAndPut() {
    database.begin();

    EntityImpl doc1 = new EntityImpl(CLASS_NAME);
    doc1.field(FIELD_NAME, 1);
    doc1.save();

    EntityImpl doc2 = new EntityImpl(CLASS_NAME);
    doc2.field(FIELD_NAME, 1);
    doc2.save();

    EntityImpl doc3 = new EntityImpl(CLASS_NAME);
    doc3.field(FIELD_NAME, 2);
    doc3.save();

    final Index index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    database.commit();

    Assert.assertEquals(3, index.getInternal().size(database));
    Assert.assertEquals(2, (fetchCollectionFromIndex(index, 1)).size());
    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 2)).size());

    database.begin();

    doc1 = database.bindToSession(doc1);
    doc2 = database.bindToSession(doc2);
    doc3 = database.bindToSession(doc3);

    doc1.delete();
    doc2.delete();
    doc3.delete();

    doc3 = new EntityImpl(CLASS_NAME);
    doc3.field(FIELD_NAME, 1);
    doc3.save();

    EntityImpl doc = new EntityImpl(CLASS_NAME);
    doc.field(FIELD_NAME, 2);
    doc.save();

    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 1)).size());
    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 2)).size());

    database.rollback();

    Assert.assertNull(database.getTransaction().getIndexChanges(INDEX_NAME));

    Assert.assertEquals(3, index.getInternal().size(database));
    Assert.assertEquals(2, (fetchCollectionFromIndex(index, 1)).size());
    Assert.assertEquals(1, (fetchCollectionFromIndex(index, 2)).size());
  }
}
