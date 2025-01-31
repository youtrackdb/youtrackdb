package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.CommitSerializationException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
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
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseDocumentTxTest extends DbTestBase {

  @Test
  public void testCountClass() throws Exception {

    var testSuperclass = db.getMetadata().getSchema().createClass("TestSuperclass");
    db.getMetadata().getSchema().createClass("TestSubclass", testSuperclass);

    db.begin();
    var toDelete = ((EntityImpl) db.newEntity("TestSubclass")).field("id", 1);
    toDelete.save();
    db.commit();

    // 1 SUB, 0 SUPER
    Assert.assertEquals(1, db.countClass("TestSubclass", false));
    Assert.assertEquals(1, db.countClass("TestSubclass", true));
    Assert.assertEquals(0, db.countClass("TestSuperclass", false));
    Assert.assertEquals(1, db.countClass("TestSuperclass", true));

    db.begin();
    try {
      ((EntityImpl) db.newEntity("TestSuperclass")).field("id", 1).save();
      ((EntityImpl) db.newEntity("TestSubclass")).field("id", 1).save();
      // 2 SUB, 1 SUPER

      Assert.assertEquals(1, db.countClass("TestSuperclass", false));
      Assert.assertEquals(3, db.countClass("TestSuperclass", true));
      Assert.assertEquals(2, db.countClass("TestSubclass", false));
      Assert.assertEquals(2, db.countClass("TestSubclass", true));

      db.bindToSession(toDelete).delete();
      // 1 SUB, 1 SUPER

      Assert.assertEquals(1, db.countClass("TestSuperclass", false));
      Assert.assertEquals(2, db.countClass("TestSuperclass", true));
      Assert.assertEquals(1, db.countClass("TestSubclass", false));
      Assert.assertEquals(1, db.countClass("TestSubclass", true));
    } finally {
      db.commit();
    }
  }

  @Test
  public void testTimezone() {

    db.set(DatabaseSession.ATTRIBUTES.TIMEZONE, "Europe/Rome");
    var newTimezone = db.get(DatabaseSession.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals("Europe/Rome", newTimezone);

    db.set(DatabaseSession.ATTRIBUTES.TIMEZONE, "foobar");
    newTimezone = db.get(DatabaseSession.ATTRIBUTES.TIMEZONE);
    Assert.assertEquals("GMT", newTimezone);
  }

  @Test(expected = CommitSerializationException.class)
  public void testSaveInvalidRid() {
    db.begin();
    var doc = (EntityImpl) db.newEntity();
    doc.field("test", new RecordId(-2, 10));
    db.save(doc);
    db.commit();
  }

  @Test
  public void testCreateClass() {
    var clazz = db.createClass("TestCreateClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClass", clazz.getName());
    var superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }
    Assert.assertNotNull(db.getMetadata().getSchema().getClass("TestCreateClass"));
    try {
      db.createClass("TestCreateClass");
      Assert.fail();
    } catch (SchemaException ex) {
    }

    var subclazz = db.createClass("TestCreateClass_subclass", "TestCreateClass");
    Assert.assertNotNull(subclazz);
    Assert.assertEquals("TestCreateClass_subclass", subclazz.getName());
    var sub_superclasses = subclazz.getSuperClasses();
    Assert.assertEquals(1, sub_superclasses.size());
    Assert.assertEquals("TestCreateClass", sub_superclasses.get(0).getName());
  }

  @Test
  public void testGetClass() {
    db.createClass("TestGetClass");

    var clazz = db.getClass("TestGetClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestGetClass", clazz.getName());
    var superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    var clazz2 = db.getClass("TestGetClass_non_existing");
    Assert.assertNull(clazz2);
  }

  @Test
  public void testDocFromJsonEmbedded() {
    Schema schema = db.getMetadata().getSchema();

    var c0 = schema.createClass("testDocFromJsonEmbedded_Class0");

    var c1 = schema.createClass("testDocFromJsonEmbedded_Class1");
    c1.createProperty(db, "account", PropertyType.STRING);
    c1.createProperty(db, "meta", PropertyType.EMBEDDED, c0);

    db.begin();
    var doc = (EntityImpl) db.newEntity("testDocFromJsonEmbedded_Class1");

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

    db.save(doc);
    db.commit();

    try (var result = db.query("select from testDocFromJsonEmbedded_Class0")) {
      Assert.assertEquals(0, result.stream().count());
    }

    try (var result = db.query("select from testDocFromJsonEmbedded_Class1")) {
      Assert.assertTrue(result.hasNext());
      var item = result.next().getEntity().get();
      EntityImpl meta = item.getProperty("meta");
      Assert.assertEquals("testDocFromJsonEmbedded_Class0", meta.getClassName());
      Assert.assertEquals("0:0:0:0:0:0:0:1", meta.field("ip"));
    }
  }

  @Test
  public void testCreateClassIfNotExists() {
    db.createClass("TestCreateClassIfNotExists");

    var clazz = db.createClassIfNotExist("TestCreateClassIfNotExists");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateClassIfNotExists", clazz.getName());
    var superclasses = clazz.getSuperClasses();
    if (superclasses != null) {
      assertTrue(superclasses.isEmpty());
    }

    var clazz2 = db.createClassIfNotExist("TestCreateClassIfNotExists_non_existing");
    Assert.assertNotNull(clazz2);
    Assert.assertEquals("TestCreateClassIfNotExists_non_existing", clazz2.getName());
    var superclasses2 = clazz2.getSuperClasses();
    if (superclasses2 != null) {
      assertTrue(superclasses2.isEmpty());
    }
  }

  @Test
  public void testCreateVertexClass() {
    var clazz = db.createVertexClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateVertexClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateVertexClass", clazz.getName());
    var superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("V", superclasses.get(0).getName());
  }

  @Test
  public void testCreateEdgeClass() {
    var clazz = db.createEdgeClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);

    clazz = db.getClass("TestCreateEdgeClass");
    Assert.assertNotNull(clazz);
    Assert.assertEquals("TestCreateEdgeClass", clazz.getName());
    var superclasses = clazz.getSuperClasses();
    Assert.assertEquals(1, superclasses.size());
    Assert.assertEquals("E", superclasses.get(0).getName());
  }

  @Test
  public void testVertexProperty() {
    var className = "testVertexProperty";
    db.createClass(className, "V");

    db.begin();
    var doc1 = db.newVertex(className);
    doc1.setProperty("name", "a");
    doc1.save();

    var doc2 = db.newVertex(className);
    doc2.setProperty("name", "b");
    doc2.setProperty("linked", doc1);
    doc2.save();
    db.commit();

    try (var rs = db.query("SELECT FROM " + className + " WHERE name = 'b'")) {
      Assert.assertTrue(rs.hasNext());
      var res = rs.next();

      var linkedVal = res.getProperty("linked");
      Assert.assertTrue(linkedVal instanceof Identifiable);
      Assert.assertTrue(
          db.load(((Identifiable) linkedVal).getIdentity()) instanceof Identifiable);

      Assert.assertTrue(res.asEntity().getProperty("linked") instanceof Vertex);
    }
  }

  @Test
  public void testLinkEdges() {
    var vertexClass = "testVertex";
    var edgeClass = "testEdge";
    var vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");
    vc.createProperty(db, "out_testEdge", PropertyType.LINK);
    vc.createProperty(db, "in_testEdge", PropertyType.LINK);

    db.begin();
    var doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();
    db.commit();

    db.begin();
    var doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();
    db.newRegularEdge(db.bindToSession(doc1), doc2, "testEdge").save();
    db.commit();

    try (var rs = db.query("SELECT out() as o FROM " + vertexClass)) {
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
    var vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");

    vc.createProperty(db, "out_testEdge", PropertyType.LINKBAG);
    vc.createProperty(db, "in_testEdge", PropertyType.LINK);

    db.begin();
    var doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    var doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    var doc3 = db.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    db.newRegularEdge(doc1, doc2, "testEdge").save();
    db.newRegularEdge(doc1, doc3, "testEdge").save();
    db.commit();

    try (var rs = db.query("SELECT out() as o FROM " + vertexClass)) {
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
    var vc = db.createClass(vertexClass, "V");
    db.createClass(edgeClass, "E");
    vc.createProperty(db, "out_testEdge", PropertyType.LINK);
    vc.createProperty(db, "in_testEdge", PropertyType.LINK);
    var doc1 = db.newVertex(vertexClass);
    doc1.setProperty("name", "first");
    doc1.save();

    var doc2 = db.newVertex(vertexClass);
    doc2.setProperty("name", "second");
    doc2.save();

    var doc3 = db.newVertex(vertexClass);
    doc3.setProperty("name", "third");
    doc3.save();

    db.newRegularEdge(doc1, doc2, "testEdge");
    db.newRegularEdge(doc1, doc3, "testEdge");
  }

  @Test
  public void selectDescTest() {
    var className = "bar";
    Schema schema = db.getMetadata().getSchema();
    schema.createClass(className, 1, schema.getClass(SchemaClass.VERTEX_CLASS_NAME));
    db.begin();

    var document = (EntityImpl) db.newEntity(className);
    document.save();
    var reverseIterator =
        new RecordIteratorClassDescendentOrder<EntityImpl>(db, db, className, true);
    Assert.assertTrue(reverseIterator.hasNext());
    Assert.assertEquals(document, reverseIterator.next());
  }
}
