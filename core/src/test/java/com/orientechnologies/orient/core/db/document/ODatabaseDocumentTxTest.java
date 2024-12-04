package com.orientechnologies.orient.core.db.document;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OCommitSerializationException;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClassDescendentOrder;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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
    YTDocument toDelete = new YTDocument("TestSubclass").field("id", 1);
    toDelete.save();
    db.commit();

    // 1 SUB, 0 SUPER
    Assert.assertEquals(db.countClass("TestSubclass", false), 1);
    Assert.assertEquals(db.countClass("TestSubclass", true), 1);
    Assert.assertEquals(db.countClass("TestSuperclass", false), 0);
    Assert.assertEquals(db.countClass("TestSuperclass", true), 1);

    db.begin();
    try {
      new YTDocument("TestSuperclass").field("id", 1).save();
      new YTDocument("TestSubclass").field("id", 1).save();
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

  @Test(expected = OCommitSerializationException.class)
  public void testSaveInvalidRid() {
    db.begin();
    YTDocument doc = new YTDocument();
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
    } catch (OSchemaException ex) {
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
    YTDocument doc = new YTDocument("testDocFromJsonEmbedded_Class1");

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
      YTEntity item = result.next().getElement().get();
      YTDocument meta = item.getProperty("meta");
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
    YTVertex doc1 = db.newVertex(className);
    doc1.setProperty("name", "a");
    doc1.save();

    YTVertex doc2 = db.newVertex(className);
    doc2.setProperty("name", "b");
    doc2.setProperty("linked", doc1);
    doc2.save();
    db.commit();

    try (OResultSet rs = db.query("SELECT FROM " + className + " WHERE name = 'b'")) {
      Assert.assertTrue(rs.hasNext());
      OResult res = rs.next();

      Object linkedVal = res.getProperty("linked");
      Assert.assertTrue(linkedVal instanceof YTIdentifiable);
      Assert.assertTrue(
          db.load(((YTIdentifiable) linkedVal).getIdentity()) instanceof YTIdentifiable);

      Assert.assertTrue(res.toElement().getProperty("linked") instanceof YTVertex);
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
    YTVertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();
    db.commit();

    db.begin();
    YTVertex doc2 = db.newVertex(vertexClass);
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
    YTClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");

    vc.createProperty(db, "out_testEdge", YTType.LINKBAG);
    vc.createProperty(db, "in_testEdge", YTType.LINK);

    db.begin();
    YTVertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    YTVertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    YTVertex doc3 = db.newVertex(vertexClass);
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
    YTClass vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");
    vc.createProperty(db, "out_testEdge", YTType.LINK);
    vc.createProperty(db, "in_testEdge", YTType.LINK);
    YTVertex doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    YTVertex doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    YTVertex doc3 = db.newVertex(vertexClass);
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

    YTDocument document = new YTDocument(className);
    document.save();
    ORecordIteratorClassDescendentOrder<YTDocument> reverseIterator =
        new ORecordIteratorClassDescendentOrder<YTDocument>(db, db, className, true);
    Assert.assertTrue(reverseIterator.hasNext());
    Assert.assertEquals(document, reverseIterator.next());
  }
}
