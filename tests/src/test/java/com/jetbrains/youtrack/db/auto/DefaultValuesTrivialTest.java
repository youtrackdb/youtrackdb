package com.jetbrains.youtrack.db.auto;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Date;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @since 3/3/2015
 */
public class DefaultValuesTrivialTest {

  private static final int DOCUMENT_COUNT = 50;

  private DatabaseSessionInternal session;

  @BeforeMethod
  public void before() {
    //noinspection deprecation
    session = new DatabaseDocumentTx("memory:" + DefaultValuesTrivialTest.class.getSimpleName());
    //noinspection deprecation
    session.create();
  }

  @AfterMethod
  public void after() {
    //noinspection deprecation
    session.drop();
  }

  @Test
  public void test() {

    // create example schema
    Schema schema = session.getMetadata().getSchema();
    var classPerson = schema.createClass("Person");

    classPerson.createProperty(session, "name", PropertyType.STRING);
    classPerson.createProperty(session, "join_date", PropertyType.DATETIME)
        .setDefaultValue(session, "sysdate()");
    classPerson.createProperty(session, "active", PropertyType.BOOLEAN)
        .setDefaultValue(session, "true");

    var dtStart = getDatabaseSysdate(session);

    var docs = new EntityImpl[DOCUMENT_COUNT];
    for (var i = 0; i < DOCUMENT_COUNT; ++i) {
      session.begin();
      var doc = ((EntityImpl) session.newEntity("Person"));
      doc.field("name", "autoGeneratedName #" + i);
      doc.save();
      session.commit();

      docs[i] = doc;
    }

    var dtAfter = getDatabaseSysdate(session);
    for (var i = 0; i < DOCUMENT_COUNT; ++i) {
      final var doc = docs[i];

      try {
        //
        Date dt = doc.field("join_date", PropertyType.DATETIME);

        var isInRange = (!dt.before(dtStart)) && (!dt.after(dtAfter));
        Assert.assertTrue(isInRange);

        //
        boolean active = doc.field("active", PropertyType.BOOLEAN);
        Assert.assertTrue(active);
      } catch (Exception ex) {
        ex.printStackTrace();
        Assert.fail();
      }
    }
  }

  private static Date getDatabaseSysdate(DatabaseSession database) {
    try (var dates = database.query("SELECT sysdate() as sysdate")) {
      return dates.next().getProperty("sysdate");
    }
  }

  @Test
  public void testDefaultValueConversion() {
    Schema schema = session.getMetadata().getSchema();
    var classPerson = schema.createClass("Person");
    classPerson.createProperty(session, "users", PropertyType.LINKSET)
        .setDefaultValue(session, "[#5:1]");

    var doc = ((EntityImpl) session.newEntity("Person"));

    session.begin();
    var record = session.save(doc);
    session.commit();

    EntityImpl doc1 = session.load(record.getIdentity());
    Set<Identifiable> rids = doc1.field("users");
    assertEquals(rids.size(), 1);
    assertEquals(rids.iterator().next(), new RecordId(5, 1));
  }

  @Test
  public void testPrepopulation() {
    // create example schema
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("ClassA");

    classA.createProperty(session, "name", PropertyType.STRING)
        .setDefaultValue(session, "default name");
    classA.createProperty(session, "date", PropertyType.DATETIME)
        .setDefaultValue(session, "sysdate()");
    classA.createProperty(session, "active", PropertyType.BOOLEAN)
        .setDefaultValue(session, "true");

    {
      var doc = ((EntityImpl) session.newEntity(classA));
      assertEquals("default name", doc.field("name"));
      assertNotNull(doc.field("date"));
      assertEquals((Boolean) true, doc.field("active"));
    }

    {
      var doc = ((EntityImpl) session.newEntity(classA.getName(session)));
      assertNull(doc.field("name"));
      assertNull(doc.field("date"));
      assertNull(doc.field("active"));
      assertEquals("default name", doc.field("name"));
      assertNotNull(doc.field("date"));
      assertEquals((Boolean) true, doc.field("active"));
    }

    {
      var doc = ((EntityImpl) session.newEntity());
      assertNull(doc.field("name"));
      assertNull(doc.field("date"));
      assertNull(doc.field("active"));
      doc.setClassNameIfExists(classA.getName(session));
      assertEquals("default name", doc.field("name"));
      assertNotNull(doc.field("date"));
      assertEquals((Boolean) true, doc.field("active"));
    }
  }

  @Test
  public void testPrepopulationIndex() {
    // create example schema
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("ClassA");

    var prop = classA.createProperty(session, "name", PropertyType.STRING);
    prop.setDefaultValue(session, "default name");
    prop.createIndex(session, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    {
      var doc = ((EntityImpl) session.newEntity(classA));
      assertEquals("default name", doc.field("name"));
      session.begin();
      session.save(doc);
      session.commit();
      try (var stream = session.getIndex("ClassA.name").getInternal()
          .getRids(session, "default name")) {
        assertEquals(1, stream.count());
      }
    }
  }

  @Test
  public void testPrepopulationIndexTx() {

    // create example schema
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("ClassA");

    var prop = classA.createProperty(session, "name", PropertyType.STRING);
    prop.setDefaultValue(session, "default name");
    prop.createIndex(session, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    {
      session.begin();
      var doc = ((EntityImpl) session.newEntity(classA));
      assertEquals("default name", doc.field("name"));

      session.begin();
      session.save(doc);
      session.commit();

      var index = session.getIndex("ClassA.name");
      try (var stream = index.getInternal()
          .getRids(session, "default name")) {
        assertEquals(1, stream.count());
      }
      session.commit();

      index = session.getIndex("ClassA.name");
      try (var stream = index.getInternal().getRids(session, "default name")) {
        assertEquals(1, stream.count());
      }
    }
  }

  @Test
  public void testPrepopulationMultivalueIndex() {

    // create example schema
    Schema schema = session.getMetadata().getSchema();
    var classA = schema.createClass("ClassA");

    var prop = classA.createProperty(session, "name", PropertyType.STRING);
    prop.setDefaultValue(session, "default name");
    classA.createProperty(session, "value", PropertyType.STRING);
    classA.createIndex(session, "multi", SchemaClass.INDEX_TYPE.NOTUNIQUE, "value",
        "name");
    var index = session.getIndex("multi");

    {
      var doc = ((EntityImpl) session.newEntity(classA));
      assertEquals("default name", doc.field("name"));
      doc.field("value", "1");

      session.begin();
      session.save(doc);
      session.commit();

      try (var stream = index.getInternal().getRids(session, new CompositeKey("1"))) {
        assertEquals(1, stream.count());
      }
    }
    {
      var doc = ((EntityImpl) session.newEntity(classA));
      assertEquals("default name", doc.field("name"));
      doc.field("value", "2");

      session.begin();
      session.save(doc);
      session.commit();

      try (var stream = index.getInternal().getRids(session, new CompositeKey("2"))) {
        assertEquals(1, stream.count());
      }
    }
    try (var stream = index.getInternal().getRids(session, new CompositeKey("3"))) {
      assertEquals(0, stream.count());
    }
  }
}
