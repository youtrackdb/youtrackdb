package com.orientechnologies.core.tx;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
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
  private YTDatabaseSessionInternal database;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    database =
        (YTDatabaseSessionInternal)
            youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass cls = schema.createClass(CLASS_NAME);
    cls.createProperty(database, FIELD_NAME, YTType.INTEGER);
    cls.createIndex(database, INDEX_NAME, YTClass.INDEX_TYPE.NOTUNIQUE, FIELD_NAME);
  }

  @After
  public void after() {
    database.close();
    youTrackDB.close();
  }

  @Test
  public void testMultiplePut() {
    database.begin();

    final OIndex index =
        database.getMetadata().getIndexManagerInternal().getIndex(database, INDEX_NAME);

    YTEntityImpl doc = new YTEntityImpl(CLASS_NAME);
    doc.field(FIELD_NAME, 1);
    doc.save();

    YTEntityImpl doc1 = new YTEntityImpl(CLASS_NAME);
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

  private Collection<YTRID> fetchCollectionFromIndex(OIndex index, int key) {
    try (Stream<YTRID> stream = index.getInternal().getRids(database, key)) {
      return stream.collect(Collectors.toList());
    }
  }

  @Test
  public void testClearAndPut() {
    database.begin();

    YTEntityImpl doc1 = new YTEntityImpl(CLASS_NAME);
    doc1.field(FIELD_NAME, 1);
    doc1.save();

    YTEntityImpl doc2 = new YTEntityImpl(CLASS_NAME);
    doc2.field(FIELD_NAME, 1);
    doc2.save();

    YTEntityImpl doc3 = new YTEntityImpl(CLASS_NAME);
    doc3.field(FIELD_NAME, 2);
    doc3.save();

    final OIndex index =
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

    doc3 = new YTEntityImpl(CLASS_NAME);
    doc3.field(FIELD_NAME, 1);
    doc3.save();

    YTEntityImpl doc = new YTEntityImpl(CLASS_NAME);
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
