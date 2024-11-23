package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.Test;

public class OClassImplTest extends BaseMemoryInternalDatabase {

  /**
   * If class was not abstract and we call {@code setAbstract(false)} clusters should not be
   * changed.
   *
   * @throws Exception
   */
  @Test
  public void testSetAbstractClusterNotChanged() throws Exception {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createClass("Test1");
    final int oldClusterId = oClass.getDefaultClusterId();

    oClass.setAbstract(db, false);

    assertEquals(oClass.getDefaultClusterId(), oldClusterId);
  }

  /**
   * If class was abstract and we call {@code setAbstract(false)} a new non default cluster should
   * be created.
   *
   * @throws Exception
   */
  @Test
  public void testSetAbstractShouldCreateNewClusters() throws Exception {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createAbstractClass("Test2");

    oClass.setAbstract(db, false);

    assertNotEquals(-1, oClass.getDefaultClusterId());
    assertNotEquals(oClass.getDefaultClusterId(), db.getDefaultClusterId());
  }

  @Test
  public void testCreateNoLinkedClass() {
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createClass("Test21");
    oClass.createProperty(db, "some", OType.LINKLIST, (OClass) null);
    oClass.createProperty(db, "some2", OType.LINKLIST, (OClass) null, true);

    assertNotNull(oClass.getProperty("some"));
    assertNotNull(oClass.getProperty("some2"));
  }

  @Test(expected = OSchemaException.class)
  public void testCreatePropertyFailOnExistingData() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test3");

    db.executeInTx(
        () -> {
          ODocument document = new ODocument("Test3");
          document.field("some", "String");
          db.save(document);
        });

    oClass.createProperty(db, "some", OType.INTEGER);
  }

  @Test(expected = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkList() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test4");

    db.executeInTx(
        () -> {
          ODocument document = new ODocument("Test4");
          ArrayList<ODocument> list = new ArrayList<ODocument>();
          list.add(new ODocument("Test4"));
          document.field("some", list);
          db.save(document);
        });

    oClass.createProperty(db, "some", OType.EMBEDDEDLIST);
  }

  @Test(expected = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkSet() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test5");

    db.executeInTx(
        () -> {
          ODocument document = new ODocument("Test5");
          Set<ODocument> list = new HashSet<ODocument>();
          list.add(new ODocument("Test5"));
          document.field("somelinkset", list);
          db.save(document);
        });

    oClass.createProperty(db, "somelinkset", OType.EMBEDDEDSET);
  }

  @Test(expected = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddetSet() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test6");

    db.executeInTx(
        () -> {
          ODocument document = new ODocument("Test6");
          Set<ODocument> list = new HashSet<ODocument>();
          list.add(new ODocument("Test6"));
          document.field("someembededset", list, OType.EMBEDDEDSET);
          db.save(document);
        });

    oClass.createProperty(db, "someembededset", OType.LINKSET);
  }

  @Test(expected = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddedList() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test7");

    db.executeInTx(
        () -> {
          ODocument document = new ODocument("Test7");
          List<ODocument> list = new ArrayList<ODocument>();
          list.add(new ODocument("Test7"));
          document.field("someembeddedlist", list, OType.EMBEDDEDLIST);
          db.save(document);
        });

    oClass.createProperty(db, "someembeddedlist", OType.LINKLIST);
  }

  @Test(expected = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddedMap() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test8");

    db.executeInTx(
        () -> {
          ODocument document = new ODocument("Test8");
          Map<String, ODocument> map = new HashMap<>();
          map.put("test", new ODocument("Test8"));
          document.field("someembededmap", map, OType.EMBEDDEDMAP);
          db.save(document);
        });

    oClass.createProperty(db, "someembededmap", OType.LINKMAP);
  }

  @Test(expected = OSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkMap() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test9");
    oSchema.createClass("Test8");

    db.executeInTx(
        () -> {
          ODocument document = new ODocument("Test9");
          Map<String, ODocument> map = new HashMap<String, ODocument>();
          map.put("test", new ODocument("Test8"));
          document.field("somelinkmap", map, OType.LINKMAP);
          db.save(document);
        });

    oClass.createProperty(db, "somelinkmap", OType.EMBEDDEDMAP);
  }

  @Test
  public void testCreatePropertyCastable() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test10");

    var rid =
        db.computeInTx(
            () -> {
              ODocument document = new ODocument("Test10");
              // TODO add boolan and byte
              document.field("test1", (short) 1);
              document.field("test2", 1);
              document.field("test3", 4L);
              document.field("test4", 3.0f);
              document.field("test5", 3.0D);
              document.field("test6", 4);
              db.save(document);
              return document.getIdentity();
            });

    oClass.createProperty(db, "test1", OType.INTEGER);
    oClass.createProperty(db, "test2", OType.LONG);
    oClass.createProperty(db, "test3", OType.DOUBLE);
    oClass.createProperty(db, "test4", OType.DOUBLE);
    oClass.createProperty(db, "test5", OType.DECIMAL);
    oClass.createProperty(db, "test6", OType.FLOAT);

    ODocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1"), OType.INTEGER);
    assertTrue(doc1.field("test1") instanceof Integer);
    assertEquals(doc1.fieldType("test2"), OType.LONG);
    assertTrue(doc1.field("test2") instanceof Long);
    assertEquals(doc1.fieldType("test3"), OType.DOUBLE);
    assertTrue(doc1.field("test3") instanceof Double);
    assertEquals(doc1.fieldType("test4"), OType.DOUBLE);
    assertTrue(doc1.field("test4") instanceof Double);
    assertEquals(doc1.fieldType("test5"), OType.DECIMAL);
    assertTrue(doc1.field("test5") instanceof BigDecimal);
    assertEquals(doc1.fieldType("test6"), OType.FLOAT);
    assertTrue(doc1.field("test6") instanceof Float);
  }

  @Test
  public void testCreatePropertyCastableColection() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test11");

    var rid =
        db.computeInTx(
            () -> {
              ODocument document = new ODocument("Test11");
              document.field("test1", new ArrayList<ODocument>(), OType.EMBEDDEDLIST);
              document.field("test2", new ArrayList<ODocument>(), OType.LINKLIST);
              document.field("test3", new HashSet<ODocument>(), OType.EMBEDDEDSET);
              document.field("test4", new HashSet<ODocument>(), OType.LINKSET);
              document.field("test5", new HashMap<String, ODocument>(), OType.EMBEDDEDMAP);
              document.field("test6", new HashMap<String, ODocument>(), OType.LINKMAP);
              db.save(document);
              return document.getIdentity();
            });

    oClass.createProperty(db, "test1", OType.LINKLIST);
    oClass.createProperty(db, "test2", OType.EMBEDDEDLIST);
    oClass.createProperty(db, "test3", OType.LINKSET);
    oClass.createProperty(db, "test4", OType.EMBEDDEDSET);
    oClass.createProperty(db, "test5", OType.LINKMAP);
    oClass.createProperty(db, "test6", OType.EMBEDDEDMAP);

    ODocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1"), OType.LINKLIST);
    assertEquals(doc1.fieldType("test2"), OType.EMBEDDEDLIST);
    assertEquals(doc1.fieldType("test3"), OType.LINKSET);
    assertEquals(doc1.fieldType("test4"), OType.EMBEDDEDSET);
    assertEquals(doc1.fieldType("test5"), OType.LINKMAP);
    assertEquals(doc1.fieldType("test6"), OType.EMBEDDEDMAP);
  }

  @Test
  public void testCreatePropertyIdKeep() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test12");
    OProperty prop = oClass.createProperty(db, "test2", OType.STRING);
    Integer id = prop.getId();
    oClass.dropProperty(db, "test2");
    prop = oClass.createProperty(db, "test2", OType.STRING);
    assertEquals(id, prop.getId());
  }

  @Test
  public void testRenameProperty() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test13");
    OProperty prop = oClass.createProperty(db, "test1", OType.STRING);
    Integer id = prop.getId();
    prop.setName(db, "test2");
    assertNotEquals(id, prop.getId());
  }

  @Test
  public void testChangeTypeProperty() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test14");
    OProperty prop = oClass.createProperty(db, "test1", OType.SHORT);
    Integer id = prop.getId();
    prop.setType(db, OType.INTEGER);
    assertNotEquals(id, prop.getId());
  }

  @Test
  public void testRenameBackProperty() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test15");
    OProperty prop = oClass.createProperty(db, "test1", OType.STRING);
    Integer id = prop.getId();
    prop.setName(db, "test2");
    assertNotEquals(id, prop.getId());
    prop.setName(db, "test1");
    assertEquals(id, prop.getId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetUncastableType() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test16");
    OProperty prop = oClass.createProperty(db, "test1", OType.STRING);
    prop.setType(db, OType.INTEGER);
  }

  @Test
  public void testFindById() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test17");
    OProperty prop = oClass.createProperty(db, "testaaa", OType.STRING);
    OGlobalProperty global = oSchema.getGlobalPropertyById(prop.getId());

    assertEquals(prop.getId(), global.getId());
    assertEquals(prop.getName(), global.getName());
    assertEquals(prop.getType(), global.getType());
  }

  @Test
  public void testFindByIdDrop() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test18");
    OProperty prop = oClass.createProperty(db, "testaaa", OType.STRING);
    Integer id = prop.getId();
    oClass.dropProperty(db, "testaaa");
    OGlobalProperty global = oSchema.getGlobalPropertyById(id);

    assertEquals(id, global.getId());
    assertEquals("testaaa", global.getName());
    assertEquals(OType.STRING, global.getType());
  }

  @Test
  public void testChangePropertyTypeCastable() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test19");

    oClass.createProperty(db, "test1", OType.SHORT);
    oClass.createProperty(db, "test2", OType.INTEGER);
    oClass.createProperty(db, "test3", OType.LONG);
    oClass.createProperty(db, "test4", OType.FLOAT);
    oClass.createProperty(db, "test5", OType.DOUBLE);
    oClass.createProperty(db, "test6", OType.INTEGER);

    var rid =
        db.computeInTx(
            () -> {
              ODocument document = new ODocument("Test19");
              // TODO add boolean and byte
              document.field("test1", (short) 1);
              document.field("test2", 1);
              document.field("test3", 4L);
              document.field("test4", 3.0f);
              document.field("test5", 3.0D);
              document.field("test6", 4);
              db.save(document);
              return document.getIdentity();
            });

    oClass.getProperty("test1").setType(db, OType.INTEGER);
    oClass.getProperty("test2").setType(db, OType.LONG);
    oClass.getProperty("test3").setType(db, OType.DOUBLE);
    oClass.getProperty("test4").setType(db, OType.DOUBLE);
    oClass.getProperty("test5").setType(db, OType.DECIMAL);
    oClass.getProperty("test6").setType(db, OType.FLOAT);

    ODocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1"), OType.INTEGER);
    assertTrue(doc1.field("test1") instanceof Integer);
    assertEquals(doc1.fieldType("test2"), OType.LONG);
    assertTrue(doc1.field("test2") instanceof Long);
    assertEquals(doc1.fieldType("test3"), OType.DOUBLE);
    assertTrue(doc1.field("test3") instanceof Double);
    assertEquals(doc1.fieldType("test4"), OType.DOUBLE);
    assertTrue(doc1.field("test4") instanceof Double);
    assertEquals(doc1.fieldType("test5"), OType.DECIMAL);
    assertTrue(doc1.field("test5") instanceof BigDecimal);
    assertEquals(doc1.fieldType("test6"), OType.FLOAT);
    assertTrue(doc1.field("test6") instanceof Float);
  }

  @Test
  public void testChangePropertyName() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test20");

    oClass.createProperty(db, "test1", OType.SHORT);
    oClass.createProperty(db, "test2", OType.INTEGER);
    oClass.createProperty(db, "test3", OType.LONG);
    oClass.createProperty(db, "test4", OType.FLOAT);
    oClass.createProperty(db, "test5", OType.DOUBLE);
    oClass.createProperty(db, "test6", OType.INTEGER);

    var rid =
        db.computeInTx(
            () -> {
              ODocument document = new ODocument("Test20");
              // TODO add boolan and byte
              document.field("test1", (short) 1);
              document.field("test2", 1);
              document.field("test3", 4L);
              document.field("test4", 3.0f);
              document.field("test5", 3.0D);
              document.field("test6", 4);
              db.save(document);
              return document.getIdentity();
            });

    oClass.getProperty("test1").setName(db, "test1a");
    oClass.getProperty("test2").setName(db, "test2a");
    oClass.getProperty("test3").setName(db, "test3a");
    oClass.getProperty("test4").setName(db, "test4a");
    oClass.getProperty("test5").setName(db, "test5a");
    oClass.getProperty("test6").setName(db, "test6a");

    ODocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1a"), OType.SHORT);
    assertTrue(doc1.field("test1a") instanceof Short);
    assertEquals(doc1.fieldType("test2a"), OType.INTEGER);
    assertTrue(doc1.field("test2a") instanceof Integer);
    assertEquals(doc1.fieldType("test3a"), OType.LONG);
    assertTrue(doc1.field("test3a") instanceof Long);
    assertEquals(doc1.fieldType("test4a"), OType.FLOAT);
    assertTrue(doc1.field("test4a") instanceof Float);
    assertEquals(doc1.fieldType("test5a"), OType.DOUBLE);
    assertTrue(doc1.field("test5") instanceof Double);
    assertEquals(doc1.fieldType("test6a"), OType.INTEGER);
    assertTrue(doc1.field("test6a") instanceof Integer);
  }

  @Test
  public void testCreatePropertyCastableColectionNoCache() {
    final OSchema oSchema = db.getMetadata().getSchema();
    OClass oClass = oSchema.createClass("Test11bis");

    var rid =
        db.computeInTx(
            () -> {
              final ODocument document = new ODocument("Test11bis");
              document.field("test1", new ArrayList<ODocument>(), OType.EMBEDDEDLIST);
              document.field("test2", new ArrayList<ODocument>(), OType.LINKLIST);

              document.field("test3", new HashSet<ODocument>(), OType.EMBEDDEDSET);
              document.field("test4", new HashSet<ODocument>(), OType.LINKSET);
              document.field("test5", new HashMap<String, ODocument>(), OType.EMBEDDEDMAP);
              document.field("test6", new HashMap<String, ODocument>(), OType.LINKMAP);
              db.save(document);

              return document.getIdentity();
            });
    oClass.createProperty(db, "test1", OType.LINKLIST);
    oClass.createProperty(db, "test2", OType.EMBEDDEDLIST);
    oClass.createProperty(db, "test3", OType.LINKSET);
    oClass.createProperty(db, "test4", OType.EMBEDDEDSET);
    oClass.createProperty(db, "test5", OType.LINKMAP);
    oClass.createProperty(db, "test6", OType.EMBEDDEDMAP);

    ExecutorService executor = Executors.newSingleThreadExecutor();

    Future<ODocument> future =
        executor.submit(
            () -> {
              ODocument doc1 = db.copy().load(rid);
              assertEquals(doc1.fieldType("test1"), OType.LINKLIST);
              assertEquals(doc1.fieldType("test2"), OType.EMBEDDEDLIST);
              assertEquals(doc1.fieldType("test3"), OType.LINKSET);
              assertEquals(doc1.fieldType("test4"), OType.EMBEDDEDSET);
              assertEquals(doc1.fieldType("test5"), OType.LINKMAP);
              assertEquals(doc1.fieldType("test6"), OType.EMBEDDEDMAP);
              return doc1;
            });

    try {
      future.get();
    } catch (InterruptedException e) {
      e.printStackTrace();
    } catch (ExecutionException e) {
      if (e.getCause() instanceof AssertionError) {
        throw (AssertionError) e.getCause();
      }
    }

    executor.shutdown();
  }

  @Test
  public void testClassNameSyntax() {

    final OSchema oSchema = db.getMetadata().getSchema();
    assertNotNull(oSchema.createClass("OClassImplTesttestClassNameSyntax"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestClassNameSyntax"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestClassNameSyntax_"));
    assertNotNull(oSchema.createClass("_OClassImplTestte_stClassNameSyntax_"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestClassNameSyntax_1"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestClassNameSyntax_12"));
    assertNotNull(oSchema.createClass("_OClassImplTesttestCla23ssNameSyntax_12"));
    assertNotNull(oSchema.createClass("$OClassImplTesttestCla23ssNameSyntax_12"));
    assertNotNull(oSchema.createClass("OClassImplTesttestC$la23ssNameSyntax_12"));
    assertNotNull(oSchema.createClass("oOClassImplTesttestC$la23ssNameSyntax_12"));
    String[] validClassNamesSince30 = {
        "foo bar",
        "12",
        "#12",
        "12AAA",
        ",asdfasdf",
        "adsf,asdf",
        "asdf.sadf",
        ".asdf",
        "asdfaf.",
        "asdf:asdf"
    };
    for (String s : validClassNamesSince30) {
      assertNotNull(oSchema.createClass(s));
    }
  }

  @Test
  public void testTypeAny() {
    String className = "testTypeAny";
    final OSchema oSchema = db.getMetadata().getSchema();

    OClass oClass = oSchema.createClass(className);

    db.executeInTx(
        () -> {
          ODocument record = db.newInstance(className);
          record.field("name", "foo");
          record.save();
        });

    oClass.createProperty(db, "name", OType.ANY);

    try (OResultSet result = db.query("select from " + className + " where name = 'foo'")) {
      assertEquals(result.stream().count(), 1);
    }
  }

  @Test
  public void testAlterCustomAttributeInClass() {
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("TestCreateCustomAttributeClass");

    oClass.setCustom(db, "customAttribute", "value1");
    assertEquals("value1", oClass.getCustom("customAttribute"));

    oClass.setCustom(db, "custom.attribute", "value2");
    assertEquals("value2", oClass.getCustom("custom.attribute"));
  }
}
