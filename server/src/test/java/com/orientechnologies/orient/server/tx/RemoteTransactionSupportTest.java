package com.orientechnologies.orient.server.tx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.RecordDuplicatedException;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import java.util.ArrayList;
import org.junit.Test;

/**
 *
 */
public class RemoteTransactionSupportTest extends BaseServerMemoryDatabase {

  private static final String FIELD_VALUE = "VALUE";

  public void beforeTest() {
    GlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    super.beforeTest();

    db.createClass("SomeTx");
    db.createClass("SomeTx2");

    SchemaClass klass = db.createClass("IndexedTx");
    klass.createProperty(db, "name", PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    SchemaClass uniqueClass = db.createClass("UniqueIndexedTx");
    uniqueClass.createProperty(db, "name", PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);
  }

  @Test
  public void testQueryUpdateUpdatedInTxTransaction() {
    db.begin();
    EntityImpl doc = new EntityImpl("SomeTx");
    doc.setProperty("name", "Joe");
    Identifiable id = db.save(doc);
    db.commit();

    db.begin();
    EntityImpl doc2 = db.load(id.getIdentity());
    doc2.setProperty("name", "Jane");
    db.save(doc2);
    ResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals(1L, (long) result.next().getProperty("count"));
    EntityImpl doc3 = db.load(id.getIdentity());
    assertEquals("July", doc3.getProperty("name"));
    db.rollback();
  }

  @Test
  public void testResetUpdatedInTxTransaction() {
    db.begin();

    EntityImpl doc1 = new EntityImpl();
    doc1.setProperty("name", "Jane");
    db.save(doc1);
    EntityImpl doc2 = new EntityImpl("SomeTx");
    doc2.setProperty("name", "Jane");
    db.save(doc2);
    ResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertEquals(1L, (long) result.next().getProperty("count"));
    assertEquals("July", doc2.getProperty("name"));
    result.close();
  }

  @Test
  public void testQueryUpdateCreatedInTxTransaction() throws InterruptedException {
    db.begin();
    EntityImpl doc1 = new EntityImpl("SomeTx");
    doc1.setProperty("name", "Jane");
    Identifiable id = db.save(doc1);

    EntityImpl docx = new EntityImpl("SomeTx2");
    docx.setProperty("name", "Jane");
    db.save(docx);

    ResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals(1L, (long) result.next().getProperty("count"));
    EntityImpl doc2 = db.load(id.getIdentity());
    assertEquals("July", doc2.getProperty("name"));
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testRollbackTxTransaction() {
    db.begin();
    EntityImpl doc = new EntityImpl("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);
    db.commit();

    db.begin();
    EntityImpl doc1 = new EntityImpl("SomeTx");
    doc1.setProperty("name", "Jane");
    db.save(doc1);

    ResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals(2L, (long) result.next().getProperty("count"));
    result.close();
    db.rollback();

    ResultSet result1 = db.command("select count(*) from SomeTx where name='Jane'");
    assertTrue(result1.hasNext());
    assertEquals(1L, (long) result1.next().getProperty("count(*)"));
    result1.close();
  }

  @Test
  public void testRollbackTxCheckStatusTransaction() {
    db.begin();
    EntityImpl doc = new EntityImpl("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);
    db.commit();

    db.begin();
    EntityImpl doc1 = new EntityImpl("SomeTx");
    doc1.setProperty("name", "Jane");
    db.save(doc1);

    ResultSet result = db.command("select count(*) from SomeTx where name='Jane' ");
    assertTrue(result.hasNext());
    assertEquals(2L, (long) result.next().getProperty("count(*)"));

    assertTrue(db.getTransaction().isActive());
    result.close();
    db.rollback();

    ResultSet result1 = db.command("select count(*) from SomeTx where name='Jane'");
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

    ResultSet result = db.command("update SomeTx set name='July' where name = 'Jane' ");
    assertTrue(result.hasNext());
    assertEquals(1L, (long) result.next().getProperty("count"));
    result.close();
    ResultSet result1 = db.query("select from SomeTx where name='July'");
    assertTrue(result1.hasNext());
    assertEquals("July", result1.next().getProperty("name"));
    assertFalse(result.hasNext());
    result1.close();
  }

  @Test
  public void testQueryDeleteTxSQLTransaction() {
    db.begin();
    Entity someTx = db.newEntity("SomeTx");
    someTx.setProperty("name", "foo");
    someTx.save();
    db.commit();

    db.begin();
    db.command("delete from SomeTx");
    db.commit();

    ResultSet result = db.command("select from SomeTx");
    assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testDoubleSaveTransaction() {
    db.begin();
    Entity someTx = db.newEntity("SomeTx");
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
    Entity someTx = db.newEntity("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);
    db.save(someTx);
    ResultSet result = db.query("select from SomeTx");
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
    Entity someTx = db.newEntity("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);

    Entity oneMore = db.newEntity("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx);
    ResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();
    db.save(oneMore);
    db.commit();
    ResultSet result1 = db.query("select ref from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    assertEquals(someTx.getIdentity(), result1.next().getProperty("ref"));
    result1.close();
  }

  @Test
  public void testDoubleRefFlushedInTransaction() {
    db.begin();
    Entity someTx = db.newEntity("SomeTx");
    someTx.setProperty("name", "foo");
    db.save(someTx);

    Entity oneMore = db.newEntity("SomeTx");
    oneMore.setProperty("name", "bar");
    oneMore.setProperty("ref", someTx.getIdentity());

    ResultSet result = db.query("select from SomeTx");
    assertEquals(1, result.stream().count());
    result.close();

    Entity ref2 = db.newEntity("SomeTx");
    ref2.setProperty("name", "other");
    db.save(ref2);

    oneMore.setProperty("ref2", ref2.getIdentity());
    result = db.query("select from SomeTx");
    assertEquals(2, result.stream().count());
    result.close();

    db.save(oneMore);
    ResultSet result1 = db.query("select ref,ref2 from SomeTx where name='bar'");
    assertTrue(result1.hasNext());
    Result next = result1.next();
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

    EntityImpl doc = new EntityImpl("SomeTx");
    doc.setProperty("name", "Jane");
    db.save(doc);

    db.command("insert into SomeTx set name ='Jane1' ").close();
    db.command("insert into SomeTx set name ='Jane2' ").close();

    EntityImpl doc1 = new EntityImpl("SomeTx");
    doc1.setProperty("name", "Jane3");
    db.save(doc1);

    doc1 = new EntityImpl("SomeTx");
    doc1.setProperty("name", "Jane4");
    db.save(doc1);
    db.command("insert into SomeTx set name ='Jane2' ").close();

    ResultSet result = db.command("select count(*) from SomeTx");
    System.out.println(result.getExecutionPlan().toString());
    assertTrue(result.hasNext());
    assertEquals(6L, (long) result.next().getProperty("count(*)"));
    result.close();
    assertTrue(db.getTransaction().isActive());

    db.commit();

    ResultSet result1 = db.command("select count(*) from SomeTx ");
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

    Vertex v1 = db.newVertex("MyV");
    Vertex v2 = db.newVertex("MyV");
    Edge edge = v1.addEdge(v2, "MyE");
    edge.setProperty("some", "value");
    db.save(v1);
    ResultSet result1 = db.query("select out_MyE from MyV  where out_MyE is not null");
    assertTrue(result1.hasNext());
    ArrayList<Object> val = new ArrayList<>();
    val.add(edge.getIdentity());
    assertEquals(result1.next().getProperty("out_MyE"), val);
    result1.close();
  }

  @Test
  public void testRidbagsTx() {
    db.begin();

    Entity v1 = db.newEntity("SomeTx");
    Entity v2 = db.newEntity("SomeTx");
    db.save(v2);
    RidBag ridbag = new RidBag(db);
    ridbag.add(v2.getIdentity());
    v1.setProperty("rids", ridbag);
    db.save(v1);
    ResultSet result1 = db.query("select rids from SomeTx where rids is not null");
    assertTrue(result1.hasNext());
    Entity v3 = db.newEntity("SomeTx");
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

    Entity idx = db.newEntity("IndexedTx");
    idx.setProperty("name", FIELD_VALUE);
    db.save(idx);
    Entity someTx = db.newEntity("SomeTx");
    someTx.setProperty("name", "foo");
    Record id = db.save(someTx);
    try (ResultSet rs = db.query("select from ?", id)) {
    }

    db.commit();

    // nothing is found (unexpected behaviour)
    try (ResultSet rs = db.query("select * from IndexedTx where name = ?", FIELD_VALUE)) {
      assertEquals(1, rs.stream().count());
    }
  }

  @Test(expected = RecordDuplicatedException.class)
  public void testDuplicateIndexTx() {
    db.begin();

    Entity v1 = db.newEntity("UniqueIndexedTx");
    v1.setProperty("name", "a");
    db.save(v1);

    Entity v2 = db.newEntity("UniqueIndexedTx");
    v2.setProperty("name", "a");
    db.save(v2);
    db.commit();
  }
}
