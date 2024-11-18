package com.orientechnologies.orient.core.db.document;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommitSerializationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClassDescendentOrder;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collection;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ODatabaseDocumentTxTest extends BaseMemoryDatabase {

  @Test
  public void testCountClass() throws Exception {

    OClass testSuperclass = db.getMetadata().getSchema().createClass("TestSuperclass");
    db.getMetadata().getSchema().createClass("TestSubclass", testSuperclass);

    db.begin();
    ODocument toDelete = new ODocument("TestSubclass").field("id", 1).save();
    db.commit();

    // 1 SUB, 0 SUPER
    Assert.assertEquals(db.countClass("TestSubclass", false), 1);
    Assert.assertEquals(db.countClass("TestSubclass", true), 1);
    Assert.assertEquals(db.countClass("TestSuperclass", false), 0);
    Assert.assertEquals(db.countClass("TestSuperclass", true), 1);

    db.begin();
    try {
      new ODocument("TestSuperclass").field("id", 1).save();
      new ODocument("TestSubclass").field("id", 1).save();
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

    db.set(ODatabaseSession.ATTRIBUTES.TIMEZONE, "Europe/Rome");
    Object newTimezone = db.get(ODatabaseSession.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals(newTimezone, "Europe/Rome");

    db.set(ODatabaseSession.ATTRIBUTES.TIMEZONE, "foobar");
    newTimezone = db.get(ODatabaseSession.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals(newTimezone, "GMT");
  }

  @Test(expected = OCommitSerializationException.class)
  public void testSaveInvalidRid() {
    db.begin();
    ODocument doc = new ODocument();
    doc.field("test", new ORecordId(-2, 10));
    db.save(doc);
    db.commit();
  }

  @Test
  public void testCreateClass() {
    OClass clazz = db.createClass("TestCreateClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestCreateClass"));
    try {
      db.createClass("TestCreateClass");
      Assert.fail();
    } catch (OSchemaException ex) {
    }

    OClass subclazz = db.createClass("TestCreateClass_subclass", "TestCreateClass");
    Assert.assertNotNull(subclazz);
    Assert.assertEquals("TestCreateClass_subclass", subclazz.getName());
    List<OClass> sub_superclasses = subclazz.getSuperClasses();
    Assert.assertEquals(1, sub_superclasses.size());
    Assert.assertEquals("TestCreateClass", sub_superclasses.get(0).getName());
  }

  @Test
  public void testGetClass() {
    db.createClass("TestGetClass");

    OClass clazz = db.getClass("TestGetClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestGetClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    OClass clazz2 = db.getClass("TestGetClass_non_existing");
    Assert.assertNull(clazz2);
  }

  @Test
  public void testDocFromJsonEmbedded() {
    OSchema schema = db.getMetadata().getSchema();

    OClass c0 = schema.createClass("testDocFromJsonEmbedded_Class0");

    OClass c1 = schema.createClass("testDocFromJsonEmbedded_Class1");
    c1.createProperty("account", OType.STRING);
    c1.createProperty("meta", OType.EMBEDDED, c0);

    db.begin();
    ODocument doc = new ODocument("testDocFromJsonEmbedded_Class1");

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

    try (OResultSet result = db.query("select from testDocFromJsonEmbedded_Class0")) {
      Assert.assertEquals(result.stream().count(), 0);
    }

    try (OResultSet result = db.query("select from testDocFromJsonEmbedded_Class1")) {
      Assert.assertTrue(result.hasNext());
      OElement item = result.next().getElement().get();
      ODocument meta = item.getProperty("meta");
      Assert.assertEquals(meta.getClassName(), "testDocFromJsonEmbedded_Class0");
      Assert.assertEquals(meta.field("ip"), "0:0:0:0:0:0:0:1");
    }
  }

  @Test
  public void testCreateClassIfNotExists() {
    db.createClass("TestCreateClassIfNotExists");

    OClass clazz = db.createClassIfNotExist("TestCreateClassIfNotExists");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClassIfNotExists", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    OClass clazz2 = db.createClassIfNotExist("TestCreateClassIfNotExists_non_existing");
    Assert.assertNotNull(clazz2);
    Assert.assertEquals("TestCreateClassIfNotExists_non_existing", clazz2.getName());
    List<OClass> superclasses2 = clazz2.getSuperClasses();
    if (superclasses2 != null) {
      assertTrue(superclasses2.isEmpty());
    }
  }

  @Test
  public void testCreateVertexClass() {
    OClass clazz = db.createVertexClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateVertexClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("V", superclasses.get(0).getName());
  }

  @Test
  public void testCreateEdgeClass() {
    OClass clazz = db.createEdgeClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateEdgeClass", clazz.getName());
    List<OClass> superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("E", superclasses.get(0).getName());
  }

  @Test
  public void testVertexProperty() {
    String className = "testVertexProperty";
    db.createClass(className, "V");

    db.begin();
    OVertex doc1 = db.newVertex(className);
    doc1.setProperty("name", "a");
    doc1.save();

    OVertex doc2 = db.newVertex(className);
    doc2.setProperty("name", "b");
    doc2.setProperty("linked", doc1);
    doc2.save();
    db.commit();

    try (OResultSet rs = db.query("SELECT FROM " + className + " WHERE name = 'b'")) {
      Assert.assertTrue(rs.hasNext());
      OResult res = rs.next();

      Object linkedVal = res.getProperty("linked");
      Assert.assertTrue(linkedVal instanceof OIdentifiable);
      Assert.assertTrue(
          db.load(((OIdentifiable) linkedVal).getIdentity()) instanceof OIdentifiable);

      Assert.assertTrue(res.toElement().getProperty("linked") instanceof OVertex);
    }
  }

  @Test
  public void testLinkEdges() {
    String vertexClass = "testVertex";
    String edgeClass = "testEdge";
    OClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");
    vc.createProperty("out_testEdge", OType.LINK);
    vc.createProperty("in_testEdge", OType.LINK);

    db.begin();
    OVertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();
    db.commit();

    db.begin();
    OVertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();
    db.newEdge(db.bindToSession(doc1), doc2, "testEdge").save();
    db.commit();

    try (OResultSet rs = db.query("SELECT out() as o FROM " + vertexClass)) {
      Assert.assertTrue(rs.hasNext());
      OResult res = rs.next();

      Object linkedVal = res.getProperty("o");
      Assert.assertTrue(linkedVal instanceof Collection);
      Assert.assertEquals(((Collection) linkedVal).size(), 1);
    }
  }

  @Test
  public void testLinkOneSide() {
    String vertexClass = "testVertex";
    String edgeClass = "testEdge";
    OClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");

    vc.createProperty("out_testEdge", OType.LINKBAG);
    vc.createProperty("in_testEdge", OType.LINK);

    db.begin();
    OVertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    OVertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    OVertex doc3 = db.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    db.newEdge(doc1, doc2, "testEdge").save();
    db.newEdge(doc1, doc3, "testEdge").save();
    db.commit();

    try (OResultSet rs = db.query("SELECT out() as o FROM " + vertexClass)) {
      Assert.assertTrue(rs.hasNext());
      OResult res = rs.next();

      Object linkedVal = res.getProperty("o");
      Assert.assertTrue(linkedVal instanceof Collection);
      Assert.assertEquals(((Collection) linkedVal).size(), 2);
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testLinkDuplicate() {
    String vertexClass = "testVertex";
    String edgeClass = "testEdge";
    OClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");
    vc.createProperty("out_testEdge", OType.LINK);
    vc.createProperty("in_testEdge", OType.LINK);
    OVertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    OVertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    OVertex doc3 = db.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    db.newEdge(doc1, doc2, "testEdge");
    db.newEdge(doc1, doc3, "testEdge");
  }

  @Test
  public void selectDescTest() {
    String className = "bar";
    OSchema schema = db.getMetadata().getSchema();
    schema.createClass(className, 1, schema.getClass(OClass.VERTEX_CLASS_NAME));
    db.begin();

    ODocument document = new ODocument(className);
    document.save();
    ORecordIteratorClassDescendentOrder<ODocument> reverseIterator =
        new ORecordIteratorClassDescendentOrder<ODocument>(
            (ODatabaseSessionInternal) db, (ODatabaseSessionInternal) db, className, true);
    Assert.assertTrue(reverseIterator.hasNext());
    Assert.assertEquals(document, reverseIterator.next());
  }
}
