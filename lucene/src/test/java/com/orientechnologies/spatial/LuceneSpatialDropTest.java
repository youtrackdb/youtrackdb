package com.orientechnologies.spatial;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.PartitionedDatabasePool;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
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

    DatabaseSessionInternal db = new DatabaseDocumentTx(dbName);

    db.create();
    SchemaClass test = db.getMetadata().getSchema().createClass("test");
    test.createProperty(db, "name", PropertyType.STRING);
    test.createProperty(db, "latitude", PropertyType.DOUBLE).setMandatory(db, false);
    test.createProperty(db, "longitude", PropertyType.DOUBLE).setMandatory(db, false);
    db.command("create index test.name on test (name) FULLTEXT ENGINE LUCENE").close();
    db.command("create index test.ll on test (latitude,longitude) SPATIAL ENGINE LUCENE").close();
    db.close();
  }

  @Test
  public void testDeleteLuceneIndex1() {

    PartitionedDatabasePool dbPool = new PartitionedDatabasePool(dbName, "admin", "admin");

    DatabaseSessionInternal db = dbPool.acquire();
    fillDb(db, insertcount);
    db.close();

    db = dbPool.acquire();
    // @maggiolo00 Remove the next three lines and the test will not fail anymore
    SQLSynchQuery<EntityImpl> query =
        new SQLSynchQuery<EntityImpl>(
            "select from test where [latitude,longitude] WITHIN [[50.0,8.0],[51.0,9.0]]");
    List<EntityImpl> result = db.command(query).execute(db);
    Assert.assertEquals(insertcount, result.size());
    db.close();
    dbPool.close();

    // reopen to drop
    db = (DatabaseSessionInternal) new DatabaseDocumentTx(dbName).open("admin", "admin");

    db.drop();
    File dbFolder = new File(dbName);
    Assert.assertFalse(dbFolder.exists());
  }

  private void fillDb(DatabaseSession db, int count) {
    for (int i = 0; i < count; i++) {
      EntityImpl doc = new EntityImpl("test");
      doc.field("name", "TestInsert" + i);
      doc.field("latitude", 50.0 + (i * 0.000001));
      doc.field("longitude", 8.0 + (i * 0.000001));

      db.begin();
      db.save(doc);
      db.commit();
    }
    ResultSet result = db.query("select * from test");
    Assert.assertEquals(count, result.stream().count());
  }
}
