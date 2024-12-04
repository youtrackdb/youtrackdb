package com.orientechnologies.orient.server.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.YTRecordDuplicatedException;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import java.util.ArrayList;
import org.junit.Test;

/**
 *
 */
public class RemoteTransactionSupportTest extends BaseServerMemoryDatabase {

  private static final String FIELD_VALUE = "VALUE";

  public void beforeTest() {
    YTGlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    super.beforeTest();

    db.createClass("SomeTx");
    db.createClass("SomeTx2");

    YTClass klass = db.createClass("IndexedTx");
    klass.createProperty(db, "name", YTType.STRING).createIndex(db, YTClass.INDEX_TYPE.NOTUNIQUE);

    YTClass uniqueClass = db.createClass("UniqueIndexedTx");
    uniqueClass.createProperty(db, "name", YTType.STRING)
        .createIndex(db, YTClass.INDEX_TYPE.UNIQUE);
  }

  @Test
  public void testQueryUpdateUpdatedInTxTransaction() {
    db.begin();
    YTDocument doc = new YTDocument("SomeTx");
    doc.setProperty("name", "Joe");
    YTIdentifiable id = db.save(doc);
    db.commit();

    db.begin();
    YTDocument doc2 = db.load(id.getIdentity());
    doc2.setProperty("name", "Jane");
    db.save(doc2);
    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals(1L, (long) result.next().getProperty("count"));
    YTDocument doc3 = db.load(id.getIdentity());
    assertEquals("July", doc3.getProperty("name"));
    db.rollback();
  }

  @Test
  public void testResetUpdatedInTxTransaction() {
    db.begin();

    YTDocument doc1 = new YTDocument();
    doc1.setProperty("name", "Jane");
    db.save(doc1);
    YTDocument doc2 = new YTDocument("SomeTx");
    doc2.setProperty("name", "Jane");
    db.save(doc2);
    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals(1L, (long) result.next().getProperty("count"));
    assertEquals("July", doc2.getProperty("name"));
    result.close();
  }

  @Test
  public void testQueryUpdateCreatedInTxTransaction() throws InterruptedException {
    db.begin();
    YTDocument doc1 = new YTDocument("SomeTx");
    doc1.setProperty("name", "Jane");
    YTIdentifiable id = db.save(doc1);

    YTDocument docx = new YTDocument("SomeTx2");
    docx.setProperty("name", "Jane");
    db.save(docx);

    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals(1L, (long) result.next().getProperty("count"));
    YTDocument doc2 = db.load(id.getIdentity());
    assertEquals("July", doc2.getProperty("name"));
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRollbackTxTransaction() {
    db.begin();
    YTDocument doc = new YTDocument("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);
    db.commit();

    db.begin();
    YTDocument doc1 = new YTDocument("SomeTx");
    doc1.setProperty("name", "Jane");
    db.save(doc1);

    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals(2L, (long) result.next().getProperty("count"));
    result.close();
    db.rollback();

    OResultSet result1 = db.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals(1L, (long) result1.next().getProperty("count(*)"));
    result1.close();
  }

  @Test
  public void testRollbackTxCheckStatusTransaction() {
    db.begin();
    YTDocument doc = new YTDocument("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);
    db.commit();

    db.begin();
    YTDocument doc1 = new YTDocument("SomeTx");
    doc1.setProperty("name", "Jane");
    db.save(doc1);

    OResultSet result = db.command("select count(*) from SomeTx where name='Jane' ");
    assertTrue(result.hasNext());
    assertEquals(2L, (long) result.next().getProperty("count(*)"));

    assertTrue(db.getTransaction().isActive());
    result.close();
    db.rollback();

    OResultSet result1 = db.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals(1L, (long) result1.next().getProperty("count(*)"));

    assertFalse(db.getTransaction().isActive());
    result1.close();
  }

  @Test
  public void testDownloadTransactionAtStart() {
    db.begin();

    db.command("insert into SomeTx set name ='Jane' ").close();
    assertEquals(1, db.getTransaction().getEntryCount());
  }

  @Test
  public void testQueryUpdateCreatedInTxSQLTransaction() {
    db.begin();

    db.command("insert into SomeTx set name ='Jane' ").close();

    OResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals(1L, (long) result.next().getProperty("count"));
    result.close();
    OResultSet result1 = db.query("select from SomeTx where name='July'");
    assertTrue(result1.hasNext());
    assertEquals("July", result1.next().getProperty("name"));
    assertFalse(result.hasNext());
    result1.close();
  }

  @Test
  public void testQueryDeleteTxSQLTransaction() {
    db.begin();
    YTEntity someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    someTx.save();
    db.commit();

    db.begin();
    db.command("delete from SomeTx");
    db.commit();

    OResultSet result = db.command("select from SomeTx");
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testDoubleSaveTransaction() {
    db.begin();
    YTEntity someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);
    db.save(someTx);
    assertEquals(1, db.getTransaction().getEntryCount());
    assertEquals(1, db.countClass("SomeTx"));
    db.commit();
    assertEquals(1, db.countClass("SomeTx"));
  }

  @Test
  public void testDoubleSaveDoubleFlushTransaction() {
    db.begin();
    YTEntity someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);
    db.save(someTx);
    OResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    db.save(someTx);
    db.save(someTx);
    result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    assertEquals(1, db.getTransaction().getEntryCount());
    assertEquals(1, db.countClass("SomeTx"));
    db.commit();
    assertEquals(1, db.countClass("SomeTx"));
  }

  @Test
  public void testRefFlushedInTransaction() {
    db.begin();
    YTEntity someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);

    YTEntity oneMore = db.newElement("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx);
    OResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    db.save(oneMore);
    db.commit();
    OResultSet result1 = db.query("select ref from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    assertEquals(someTx.getIdentity(), result1.next().getProperty("ref"));
    result1.close();
  }

  @Test
  public void testDoubleRefFlushedInTransaction() {
    db.begin();
    YTEntity someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);

    YTEntity oneMore = db.newElement("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx.getIdentity());

    OResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();

    YTEntity ref2 = db.newElement("SomeTx");
    ref2.setProperty("name", "other");
    db.save(ref2);

    oneMore.setProperty("ref2", ref2.getIdentity());
    result = db.query("select from SomeTx");
    assertEquals(2, result.stream().count());
    result.close();

    db.save(oneMore);
    OResultSet result1 = db.query("select ref,ref2 from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    OResult next = result1.next();
    assertEquals(someTx.getIdentity(), next.getProperty("ref"));
    assertEquals(ref2.getIdentity(), next.getProperty("ref2"));
    result1.close();

    db.commit();
    result1 = db.query("select ref,ref2 from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    next = result1.next();
    assertEquals(someTx.getIdentity(), next.getProperty("ref"));
    assertEquals(ref2.getIdentity(), next.getProperty("ref2"));
    result1.close();
  }

  @Test
  public void testGenerateIdCounterTransaction() {
    db.begin();

    YTDocument doc = new YTDocument("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);

    db.command("insert into SomeTx set name ='Jane1' ").close();
    db.command("insert into SomeTx set name ='Jane2' ").close();

    YTDocument doc1 = new YTDocument("SomeTx");
    doc1.setProperty("name", "Jane3");
    db.save(doc1);

    doc1 = new YTDocument("SomeTx");
    doc1.setProperty("name", "Jane4");
    db.save(doc1);
    db.command("insert into SomeTx set name ='Jane2' ").close();

    OResultSet result = db.command("select count(*) from SomeTx");
    System.out.println(result.getExecutionPlan().toString());
    assertTrue(result.hasNext());
    assertEquals(6L, (long) result.next().getProperty("count(*)"));
    result.close();
    assertTrue(db.getTransaction().isActive());

    db.commit();

    OResultSet result1 = db.command("select count(*) from SomeTx ");
    assertTrue(result1.hasNext());
    assertEquals(6L, (long) result1.next().getProperty("count(*)"));
    result1.close();
    assertFalse(db.getTransaction().isActive());
  }

  @Test
  public void testGraphInTx() {
    db.createVertexClass("MyV");
    db.createEdgeClass("MyE");
    db.begin();

    YTVertex v1 = db.newVertex("MyV");
    YTVertex v2 = db.newVertex("MyV");
    YTEdge edge = v1.addEdge(v2, "MyE");
    edge.setProperty("some", "value");
    db.save(v1);
    OResultSet result1 = db.query("select out_MyE from MyV  where out_MyE is not null");
    assertTrue(result1.hasNext());
    ArrayList<Object> val = new ArrayList<>();
    val.add(edge.getIdentity());
    assertEquals(result1.next().getProperty("out_MyE"), val);
    result1.close();
  }

  @Test
  public void testRidbagsTx() {
    db.begin();

    YTEntity v1 = db.newElement("SomeTx");
    YTEntity v2 = db.newElement("SomeTx");
    db.save(v2);
    ORidBag ridbag = new ORidBag(db);
    ridbag.add(v2.getIdentity());
    v1.setProperty("rids", ridbag);
    db.save(v1);
    OResultSet result1 = db.query("select rids from SomeTx where rids is not null");
    assertTrue(result1.hasNext());
    YTEntity v3 = db.newElement("SomeTx");
    db.save(v3);
    ArrayList<Object> val = new ArrayList<>();
    val.add(v2.getIdentity());
    assertEquals(result1.next().getProperty("rids"), val);
    result1.close();
    result1 = db.query("select rids from SomeTx where rids is not null");
    assertTrue(result1.hasNext());
    assertEquals(result1.next().getProperty("rids"), val);
    result1.close();
  }

  @Test
  public void testProperIndexingOnDoubleInternalBegin() {
    db.begin();

    YTEntity idx = db.newElement("IndexedTx");
    idx.setProperty("name", FIELD_VALUE);
    db.save(idx);
    YTEntity someTx = db.newElement("SomeTx");
    someTx.setProperty("name", "foo");
    YTRecord id = db.save(someTx);
    try (OResultSet rs = db.query("select from ?", id)) {
    }

    db.commit();

    // nothing is found (unexpected behaviour)
    try (OResultSet rs = db.query("select * from IndexedTx where name = ?", FIELD_VALUE)) {
      assertEquals(1, rs.stream().count());
    }
  }

  @Test(expected = YTRecordDuplicatedException.class)
  public void testDuplicateIndexTx() {
    db.begin();

    YTEntity v1 = db.newElement("UniqueIndexedTx");
    v1.setProperty("name", "a");
    db.save(v1);

    YTEntity v2 = db.newElement("UniqueIndexedTx");
    v2.setProperty("name", "a");
    db.save(v2);
    db.commit();
  }
}
