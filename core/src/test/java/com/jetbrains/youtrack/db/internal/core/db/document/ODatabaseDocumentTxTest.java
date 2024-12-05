package com.jetbrains.youtrack.db.internal.core.db.document;

import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommitSerializationException;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import com.jetbrains.youtrack.db.internal.core.exception.YTSchemaException;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.ORecordIteratorClassDescendentOrder;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.Collection;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ODatabaseDocumentTxTest extends DBTestBase {

  @Test
  public void testCountClass() throws Exception {

    YTClass testSuperclass = db.getMetadata().getSchema().createClass("TestSuperclass");
    db.getMetadata().getSchema().createClass("TestSubclass", testSuperclass);

    db.begin();
    EntityImpl toDelete = new EntityImpl("TestSubclass").field("id", 1);
    toDelete.save();
    db.commit();

    // 1 SUB, 0 SUPER
    Assert.assertEquals(db.countClass("TestSubclass", false), 1);
    Assert.assertEquals(db.countClass("TestSubclass", true), 1);
    Assert.assertEquals(db.countClass("TestSuperclass", false), 0);
    Assert.assertEquals(db.countClass("TestSuperclass", true), 1);

    db.begin();
    try {
      new EntityImpl("TestSuperclass").field("id", 1).save();
      new EntityImpl("TestSubclass").field("id", 1).save();
      // 2 SUB, 1 SUPER

      Assert.assertEquals(db.countClass("TestSuperclass", false), 1);
      Assert.assertEquals(db.countClass("TestSuperclass", true), 3);
      Assert.assertEquals(db.countClass("TestSubclass", false), 2);
      Assert.assertEquals(db.countClass("TestSubclass", true), 2);

      db.bindToSession(toDelete).delete();
      // 1 SUB, 1 SUPER

      Assert.assertEquals(db.countClass("TestSuperclass", false), 1);
      Assert.assertEquals(db.countClass("TestSuperclass", true), 2);
      Assert.assertEquals(db.countClass("TestSubclass", false), 1);
      Assert.assertEquals(db.countClass("TestSubclass", true), 1);
    } finally {
      db.commit();
    }
  }

  @Test
  public void testTimezone() {

    db.set(ATTRIBUTES.TIMEZONE, "Europe/Rome");
    Object newTimezone = db.get(ATTRIBUTES.TIMEZONE);
    Assert.assertEquals(newTimezone, "Europe/Rome");

    db.set(ATTRIBUTES.TIMEZONE, "foobar");
    newTimezone = db.get(ATTRIBUTES.TIMEZONE);
    Assert.assertEquals(newTimezone, "GMT");
  }

  @Test(expected = YTCommitSerializationException.class)
  public void testSaveInvalidRid() {
    db.begin();
    EntityImpl doc = new EntityImpl();
    doc.field("test", new YTRecordId(-2, 10));
    db.save(doc);
    db.commit();
  }

  @Test
  public void testCreateClass() {
    YTClass clazz = db.createClass("TestCreateClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClass", clazz.getName());
    List<YTClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestCreateClass"));
    try {
      db.createClass("TestCreateClass");
      Assert.fail();
    } catch (YTSchemaException ex) {
    }

    YTClass subclazz = db.createClass("TestCreateClass_subclass", "TestCreateClass");
    Assert.assertNotNull(subclazz);
    Assert.assertEquals("TestCreateClass_subclass", subclazz.getName());
    List<YTClass> sub_superclasses = subclazz.getSuperClasses();
    Assert.assertEquals(1, sub_superclasses.size());
    Assert.assertEquals("TestCreateClass", sub_superclasses.get(0).getName());
  }

  @Test
  public void testGetClass() {
    db.createClass("TestGetClass");

    YTClass clazz = db.getClass("TestGetClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestGetClass", clazz.getName());
    List<YTClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    YTClass clazz2 = db.getClass("TestGetClass_non_existing");
    Assert.assertNull(clazz2);
  }

  @Test
  public void testDocFromJsonEmbedded() {
    YTSchema schema = db.getMetadata().getSchema();

    YTClass c0 = schema.createClass("testDocFromJsonEmbedded_Class0");

    YTClass c1 = schema.createClass("testDocFromJsonEmbedded_Class1");
    c1.createProperty(db, "account", YTType.STRING);
    c1.createProperty(db, "meta", YTType.EMBEDDED, c0);

    db.begin();
    EntityImpl doc = new EntityImpl("testDocFromJsonEmbedded_Class1");

    doc.fromJSON(
        "{\n"
            + "    \"account\": \"#25:0\",\n"
            + "    "
            + "\"meta\": {"
            + "   \"created\": \"2016-10-03T21:10:21.77-07:00\",\n"
            + "        \"ip\": \"0:0:0:0:0:0:0:1\",\n"
            + "   \"contentType\": \"application/x-www-form-urlencoded\","
            + "   \"userAgent\": \"PostmanRuntime/2.5.2\""
            + "},"
            + "\"data\": \"firstName=Jessica&lastName=Smith\"\n"
            + "}");

    db.save(doc);
    db.commit();

    try (YTResultSet result = db.query("select from testDocFromJsonEmbedded_Class0")) {
      Assert.assertEquals(result.stream().count(), 0);
    }

    try (YTResultSet result = db.query("select from testDocFromJsonEmbedded_Class1")) {
      Assert.assertTrue(result.hasNext());
      Entity item = result.next().getEntity().get();
      EntityImpl meta = item.getProperty("meta");
      Assert.assertEquals(meta.getClassName(), "testDocFromJsonEmbedded_Class0");
      Assert.assertEquals(meta.field("ip"), "0:0:0:0:0:0:0:1");
    }
  }

  @Test
  public void testCreateClassIfNotExists() {
    db.createClass("TestCreateClassIfNotExists");

    YTClass clazz = db.createClassIfNotExist("TestCreateClassIfNotExists");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClassIfNotExists", clazz.getName());
    List<YTClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    YTClass clazz2 = db.createClassIfNotExist("TestCreateClassIfNotExists_non_existing");
    Assert.assertNotNull(clazz2);
    Assert.assertEquals("TestCreateClassIfNotExists_non_existing", clazz2.getName());
    List<YTClass> superclasses2 = clazz2.getSuperClasses();
    if (superclasses2 != null) {
      assertTrue(superclasses2.isEmpty());
    }
  }

  @Test
  public void testCreateVertexClass() {
    YTClass clazz = db.createVertexClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateVertexClass", clazz.getName());
    List<YTClass> superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("V", superclasses.get(0).getName());
  }

  @Test
  public void testCreateEdgeClass() {
    YTClass clazz = db.createEdgeClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateEdgeClass", clazz.getName());
    List<YTClass> superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("E", superclasses.get(0).getName());
  }

  @Test
  public void testVertexProperty() {
    String className = "testVertexProperty";
    db.createClass(className, "V");

    db.begin();
    Vertex doc1 = db.newVertex(className);
    doc1.setProperty("name", "a");
    doc1.save();

    Vertex doc2 = db.newVertex(className);
    doc2.setProperty("name", "b");
    doc2.setProperty("linked", doc1);
    doc2.save();
    db.commit();

    try (YTResultSet rs = db.query("SELECT FROM " + className + " WHERE name = 'b'")) {
      Assert.assertTrue(rs.hasNext());
      YTResult res = rs.next();

      Object linkedVal = res.getProperty("linked");
      Assert.assertTrue(linkedVal instanceof YTIdentifiable);
      Assert.assertTrue(
          db.load(((YTIdentifiable) linkedVal).getIdentity()) instanceof YTIdentifiable);

      Assert.assertTrue(res.toEntity().getProperty("linked") instanceof Vertex);
    }
  }

  @Test
  public void testLinkEdges() {
    String vertexClass = "testVertex";
    String edgeClass = "testEdge";
    YTClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");
    vc.createProperty(db, "out_testEdge", YTType.LINK);
    vc.createProperty(db, "in_testEdge", YTType.LINK);

    db.begin();
    Vertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();
    db.commit();

    db.begin();
    Vertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();
    db.newEdge(db.bindToSession(doc1), doc2, "testEdge").save();
    db.commit();

    try (YTResultSet rs = db.query("SELECT out() as o FROM " + vertexClass)) {
      Assert.assertTrue(rs.hasNext());
      YTResult res = rs.next();

      Object linkedVal = res.getProperty("o");
      Assert.assertTrue(linkedVal instanceof Collection);
      Assert.assertEquals(((Collection) linkedVal).size(), 1);
    }
  }

  @Test
  public void testLinkOneSide() {
    String vertexClass = "testVertex";
    String edgeClass = "testEdge";
    YTClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");

    vc.createProperty(db, "out_testEdge", YTType.LINKBAG);
    vc.createProperty(db, "in_testEdge", YTType.LINK);

    db.begin();
    Vertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    Vertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    Vertex doc3 = db.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    db.newEdge(doc1, doc2, "testEdge").save();
    db.newEdge(doc1, doc3, "testEdge").save();
    db.commit();

    try (YTResultSet rs = db.query("SELECT out() as o FROM " + vertexClass)) {
      Assert.assertTrue(rs.hasNext());
      YTResult res = rs.next();

      Object linkedVal = res.getProperty("o");
      Assert.assertTrue(linkedVal instanceof Collection);
      Assert.assertEquals(((Collection) linkedVal).size(), 2);
    }
  }

  @Test(expected = YTDatabaseException.class)
  public void testLinkDuplicate() {
    String vertexClass = "testVertex";
    String edgeClass = "testEdge";
    YTClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");
    vc.createProperty(db, "out_testEdge", YTType.LINK);
    vc.createProperty(db, "in_testEdge", YTType.LINK);
    Vertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    Vertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    Vertex doc3 = db.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    db.newEdge(doc1, doc2, "testEdge");
    db.newEdge(doc1, doc3, "testEdge");
  }

  @Test
  public void selectDescTest() {
    String className = "bar";
    YTSchema schema = db.getMetadata().getSchema();
    schema.createClass(className, 1, schema.getClass(YTClass.VERTEX_CLASS_NAME));
    db.begin();

    EntityImpl document = new EntityImpl(className);
    document.save();
    ORecordIteratorClassDescendentOrder<EntityImpl> reverseIterator =
        new ORecordIteratorClassDescendentOrder<EntityImpl>(db, db, className, true);
    Assert.assertTrue(reverseIterator.hasNext());
    Assert.assertEquals(document, reverseIterator.next());
  }
}
