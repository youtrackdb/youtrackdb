package com.orientechnologies.spatial;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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

    ODatabaseSessionInternal db = new ODatabaseDocumentTx(dbName);

    db.create();
    OClass test = db.getMetadata().getSchema().createClass("test");
    test.createProperty(db, "name", OType.STRING);
    test.createProperty(db, "latitude", OType.DOUBLE).setMandatory(db, false);
    test.createProperty(db, "longitude", OType.DOUBLE).setMandatory(db, false);
    db.command("create index test.name on test (name) FULLTEXT ENGINE LUCENE").close();
    db.command("create index test.ll on test (latitude,longitude) SPATIAL ENGINE LUCENE").close();
    db.close();
  }

  @Test
  public void testDeleteLuceneIndex1() {

    OPartitionedDatabasePool dbPool = new OPartitionedDatabasePool(dbName, "admin", "admin");

    ODatabaseSessionInternal db = dbPool.acquire();
    fillDb(db, insertcount);
    db.close();

    db = dbPool.acquire();
    // @maggiolo00 Remove the next three lines and the test will not fail anymore
    OSQLSynchQuery<ODocument> query =
        new OSQLSynchQuery<ODocument>(
            "select from test where [latitude,longitude] WITHIN [[50.0,8.0],[51.0,9.0]]");
    List<ODocument> result = db.command(query).execute(db);
    Assert.assertEquals(insertcount, result.size());
    db.close();
    dbPool.close();

    // reopen to drop
    db = (ODatabaseSessionInternal) new ODatabaseDocumentTx(dbName).open("admin", "admin");

    db.drop();
    File dbFolder = new File(dbName);
    Assert.assertFalse(dbFolder.exists());
  }

  private void fillDb(ODatabaseSession db, int count) {
    for (int i = 0; i < count; i++) {
      ODocument doc = new ODocument("test");
      doc.field("name", "TestInsert" + i);
      doc.field("latitude", 50.0 + (i * 0.000001));
      doc.field("longitude", 8.0 + (i * 0.000001));

      db.begin();
      db.save(doc);
      db.commit();
    }
    OResultSet result = db.query("select * from test");
    Assert.assertEquals(count, result.stream().count());
  }
}
