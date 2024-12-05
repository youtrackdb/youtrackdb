package com.orientechnologies.spatial;

import com.jetbrains.youtrack.db.internal.core.db.OPartitionedDatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.YTDatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import java.io.File;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneSpatialDropTest {

  private int insertcount;
  private String dbName;

  @Before
  public void setUp() throws Exception {

    dbName = "plocal:./target/databases/" + this.getClass().getSimpleName();

    // @maggiolo00 set cont to 0 and the test will not fail anymore
    insertcount = 100;

    YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(dbName);

    db.create();
    YTClass test = db.getMetadata().getSchema().createClass("test");
    test.createProperty(db, "name", YTType.STRING);
    test.createProperty(db, "latitude", YTType.DOUBLE).setMandatory(db, false);
    test.createProperty(db, "longitude", YTType.DOUBLE).setMandatory(db, false);
    db.command("create index test.name on test (name) FULLTEXT ENGINE LUCENE").close();
    db.command("create index test.ll on test (latitude,longitude) SPATIAL ENGINE LUCENE").close();
    db.close();
  }

  @Test
  public void testDeleteLuceneIndex1() {

    OPartitionedDatabasePool dbPool = new OPartitionedDatabasePool(dbName, "admin", "admin");

    YTDatabaseSessionInternal db = dbPool.acquire();
    fillDb(db, insertcount);
    db.close();

    db = dbPool.acquire();
    // @maggiolo00 Remove the next three lines and the test will not fail anymore
    OSQLSynchQuery<EntityImpl> query =
        new OSQLSynchQuery<EntityImpl>(
            "select from test where [latitude,longitude] WITHIN [[50.0,8.0],[51.0,9.0]]");
    List<EntityImpl> result = db.command(query).execute(db);
    Assert.assertEquals(insertcount, result.size());
    db.close();
    dbPool.close();

    // reopen to drop
    db = (YTDatabaseSessionInternal) new YTDatabaseDocumentTx(dbName).open("admin", "admin");

    db.drop();
    File dbFolder = new File(dbName);
    Assert.assertFalse(dbFolder.exists());
  }

  private void fillDb(YTDatabaseSession db, int count) {
    for (int i = 0; i < count; i++) {
      EntityImpl doc = new EntityImpl("test");
      doc.field("name", "TestInsert" + i);
      doc.field("latitude", 50.0 + (i * 0.000001));
      doc.field("longitude", 8.0 + (i * 0.000001));

      db.begin();
      db.save(doc);
      db.commit();
    }
    YTResultSet result = db.query("select * from test");
    Assert.assertEquals(count, result.stream().count());
  }
}
