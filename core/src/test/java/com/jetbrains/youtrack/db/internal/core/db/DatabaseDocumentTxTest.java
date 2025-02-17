package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorClassDescendentOrder;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseDocumentTxTest extends DbTestBase {

  @Test
  public void testCountClass() throws Exception {
    var testSuperclass = session.getMetadata().getSchema().createClass("TestSuperclass");
    session.getMetadata().getSchema().createClass("TestSubclass", testSuperclass);

    session.begin();
    var toDelete = ((EntityImpl) session.newEntity("TestSubclass")).field("id", 1);
    toDelete.save();
    session.commit();

    // 1 SUB, 0 SUPER
    Assert.assertEquals(1, session.countClass("TestSubclass", false));
    Assert.assertEquals(1, session.countClass("TestSubclass", true));
    Assert.assertEquals(0, session.countClass("TestSuperclass", false));
    Assert.assertEquals(1, session.countClass("TestSuperclass", true));

    session.begin();
    try {
      ((EntityImpl) session.newEntity("TestSuperclass")).field("id", 1).save();
      ((EntityImpl) session.newEntity("TestSubclass")).field("id", 1).save();
      // 2 SUB, 1 SUPER

      Assert.assertEquals(1, session.countClass("TestSuperclass", false));
      Assert.assertEquals(3, session.countClass("TestSuperclass", true));
      Assert.assertEquals(2, session.countClass("TestSubclass", false));
      Assert.assertEquals(2, session.countClass("TestSubclass", true));

      session.bindToSession(toDelete).delete();
      // 1 SUB, 1 SUPER

      Assert.assertEquals(1, session.countClass("TestSuperclass", false));
      Assert.assertEquals(2, session.countClass("TestSuperclass", true));
      Assert.assertEquals(1, session.countClass("TestSubclass", false));
      Assert.assertEquals(1, session.countClass("TestSubclass", true));
    } finally {
      session.commit();
    }
  }

  @Test
  public void testTimezone() {

    session.set(DatabaseSession.ATTRIBUTES.TIMEZONE, "Europe/Rome");
    var newTimezone = session.get(DatabaseSession.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals("Europe/Rome", newTimezone);

    session.set(DatabaseSession.ATTRIBUTES.TIMEZONE, "foobar");
    newTimezone = session.get(DatabaseSession.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals("GMT", newTimezone);
  }

  @Test(expected = RecordNotFoundException.class)
  public void testSaveInvalidRid() {
    session.begin();
    var doc = (EntityImpl) session.newEntity();
    doc.field("test", new RecordId(-2, 10));
    session.save(doc);
    session.commit();
  }

  @Test
  public void testCreateClass() {
    var clazz = session.createClass("TestCreateClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClass", clazz.getName(session));
    var superclasses = clazz.getSuperClasses(session);
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }
    Assert.assertNotNull(session.getMetadata().getSchema().getClass("TestCreateClass"));
    try {
      session.createClass("TestCreateClass");
      Assert.fail();
    } catch (SchemaException ex) {
    }

    var subclazz = session.createClass("TestCreateClass_subclass", "TestCreateClass");
    Assert.assertNotNull(subclazz);
    Assert.assertEquals("TestCreateClass_subclass", subclazz.getName(session));
    var sub_superclasses = subclazz.getSuperClasses(session);
    Assert.assertEquals(1, sub_superclasses.size());
    Assert.assertEquals("TestCreateClass", sub_superclasses.getFirst().getName(session));
  }

  @Test
  public void testGetClass() {
    session.createClass("TestGetClass");

    var clazz = session.getClass("TestGetClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestGetClass", clazz.getName(session));
    var superclasses = clazz.getSuperClasses(session);
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    var clazz2 = session.getClass("TestGetClass_non_existing");
    Assert.assertNull(clazz2);
  }

  @Test
  public void testDocFromJsonEmbedded() {
    Schema schema = session.getMetadata().getSchema();

    var c0 = schema.createClass("testDocFromJsonEmbedded_Class0");

    var c1 = schema.createClass("testDocFromJsonEmbedded_Class1");
    c1.createProperty(session, "account", PropertyType.STRING);
    c1.createProperty(session, "meta", PropertyType.EMBEDDED, c0);

    session.begin();
    var doc = (EntityImpl) session.newEntity("testDocFromJsonEmbedded_Class1");

    doc.updateFromJSON(
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

    session.save(doc);
    session.commit();

    try (var result = session.query("select from testDocFromJsonEmbedded_Class0")) {
      Assert.assertEquals(0, result.stream().count());
    }

    try (var result = session.query("select from testDocFromJsonEmbedded_Class1")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next().castToEntity();
      EntityImpl meta = item.getProperty("meta");
      Assert.assertEquals("testDocFromJsonEmbedded_Class0", meta.getSchemaClassName());
      Assert.assertEquals("0:0:0:0:0:0:0:1", meta.field("ip"));
    }
  }

  @Test
  public void testCreateClassIfNotExists() {
    session.createClass("TestCreateClassIfNotExists");

    var clazz = session.createClassIfNotExist("TestCreateClassIfNotExists");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClassIfNotExists", clazz.getName(session));
    var superclasses = clazz.getSuperClasses(session);
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    var clazz2 = session.createClassIfNotExist("TestCreateClassIfNotExists_non_existing");
    Assert.assertNotNull(clazz2);
    Assert.assertEquals("TestCreateClassIfNotExists_non_existing", clazz2.getName(session));
    var superclasses2 = clazz2.getSuperClasses(session);
    if (superclasses2 != null) {
      assertTrue(superclasses2.isEmpty());
    }
  }

  @Test
  public void testCreateVertexClass() {
    var clazz = session.createVertexClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);

    clazz = session.getClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateVertexClass", clazz.getName(session));
    var superclasses = clazz.getSuperClasses(session);
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("V", superclasses.getFirst().getName(session));
  }

  @Test
  public void testCreateEdgeClass() {
    var clazz = session.createEdgeClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);

    clazz = session.getClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateEdgeClass", clazz.getName(session));
    var superclasses = clazz.getSuperClasses(session);
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("E", superclasses.getFirst().getName(session));
  }

  @Test
  public void testVertexProperty() {
    var className = "testVertexProperty";
    session.createClass(className, "V");

    session.begin();
    var doc1 = session.newVertex(className);
    doc1.setProperty("name", "a");
    doc1.save();

    var doc2 = session.newVertex(className);
    doc2.setProperty("name", "b");
    doc2.setProperty("linked", doc1);
    doc2.save();
    session.commit();

    try (var rs = session.query("SELECT FROM " + className + " WHERE name = 'b'")) {
      Assert.assertTrue(rs.hasNext());
      var res = rs.next();

      var linkedVal = res.getProperty("linked");
      Assert.assertTrue(linkedVal instanceof Identifiable);
      Assert.assertTrue(
          session.load(((Identifiable) linkedVal).getIdentity()) instanceof Identifiable);

      Assert.assertTrue(res.asEntity().getProperty("linked") instanceof Vertex);
    }
  }

  @Test
  public void testLinkEdges() {
    var vertexClass = "testVertex";
    var edgeClass = "testEdge";
    var vc = session.createClass(vertexClass, "V");
    session.createClass(edgeClass, "E");
    vc.createProperty(session, "out_testEdge", PropertyType.LINK);
    vc.createProperty(session, "in_testEdge", PropertyType.LINK);

    session.begin();
    var doc1 = session.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();
    session.commit();

    session.begin();
    var doc2 = session.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();
    session.newStatefulEdge(session.bindToSession(doc1), doc2, "testEdge").save();
    session.commit();

    try (var rs = session.query("SELECT out() as o FROM " + vertexClass)) {
      Assert.assertTrue(rs.hasNext());
      var res = rs.next();

      var linkedVal = res.getProperty("o");
      Assert.assertTrue(linkedVal instanceof Collection);
      Assert.assertEquals(1, ((Collection) linkedVal).size());
    }
  }

  @Test
  public void testLinkOneSide() {
    var vertexClass = "testVertex";
    var edgeClass = "testEdge";
    var vc = session.createClass(vertexClass, "V");
    session.createClass(edgeClass, "E");

    vc.createProperty(session, "out_testEdge", PropertyType.LINKBAG);
    vc.createProperty(session, "in_testEdge", PropertyType.LINK);

    session.begin();
    var doc1 = session.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    var doc2 = session.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    var doc3 = session.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    session.newStatefulEdge(doc1, doc2, "testEdge").save();
    session.newStatefulEdge(doc1, doc3, "testEdge").save();
    session.commit();

    try (var rs = session.query("SELECT out() as o FROM " + vertexClass)) {
      Assert.assertTrue(rs.hasNext());
      var res = rs.next();

      var linkedVal = res.getProperty("o");
      Assert.assertTrue(linkedVal instanceof Collection);
      Assert.assertEquals(2, ((Collection) linkedVal).size());
    }
  }

  @Test(expected = DatabaseException.class)
  public void testLinkDuplicate() {
    var vertexClass = "testVertex";
    var edgeClass = "testEdge";
    var vc = session.createClass(vertexClass, "V");
    session.createClass(edgeClass, "E");
    vc.createProperty(session, "out_testEdge", PropertyType.LINK);
    vc.createProperty(session, "in_testEdge", PropertyType.LINK);
    var doc1 = session.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    var doc2 = session.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    var doc3 = session.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    session.newStatefulEdge(doc1, doc2, "testEdge");
    session.newStatefulEdge(doc1, doc3, "testEdge");
  }

  @Test
  public void selectDescTest() {
    var className = "bar";
    Schema schema = session.getMetadata().getSchema();
    schema.createClass(className, 1, schema.getClass(SchemaClass.VERTEX_CLASS_NAME));
    session.begin();

    var document = (EntityImpl) session.newEntity(className);
    document.save();
    var reverseIterator =
        new RecordIteratorClassDescendentOrder<EntityImpl>(session, session, className, true);
    Assert.assertTrue(reverseIterator.hasNext());
    Assert.assertEquals(document, reverseIterator.next());
  }
}
