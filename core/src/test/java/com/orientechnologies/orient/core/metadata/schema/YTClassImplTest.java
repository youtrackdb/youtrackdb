package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
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

public class YTClassImplTest extends BaseMemoryInternalDatabase {

  /**
   * If class was not abstract and we call {@code setAbstract(false)} clusters should not be
   * changed.
   *
   * @throws Exception
   */
  @Test
  public void testSetAbstractClusterNotChanged() throws Exception {
    final YTSchema oSchema = db.getMetadata().getSchema();

    YTClass oClass = oSchema.createClass("Test1");
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
    final YTSchema oSchema = db.getMetadata().getSchema();

    YTClass oClass = oSchema.createAbstractClass("Test2");

    oClass.setAbstract(db, false);

    assertNotEquals(-1, oClass.getDefaultClusterId());
    assertNotEquals(oClass.getDefaultClusterId(), db.getDefaultClusterId());
  }

  @Test
  public void testCreateNoLinkedClass() {
    final YTSchema oSchema = db.getMetadata().getSchema();

    YTClass oClass = oSchema.createClass("Test21");
    oClass.createProperty(db, "some", YTType.LINKLIST, (YTClass) null);
    oClass.createProperty(db, "some2", YTType.LINKLIST, (YTClass) null, true);

    assertNotNull(oClass.getProperty("some"));
    assertNotNull(oClass.getProperty("some2"));
  }

  @Test(expected = YTSchemaException.class)
  public void testCreatePropertyFailOnExistingData() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test3");

    db.executeInTx(
        () -> {
          YTDocument document = new YTDocument("Test3");
          document.field("some", "String");
          db.save(document);
        });

    oClass.createProperty(db, "some", YTType.INTEGER);
  }

  @Test(expected = YTSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkList() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test4");

    db.executeInTx(
        () -> {
          YTDocument document = new YTDocument("Test4");
          ArrayList<YTDocument> list = new ArrayList<YTDocument>();
          list.add(new YTDocument("Test4"));
          document.field("some", list);
          db.save(document);
        });

    oClass.createProperty(db, "some", YTType.EMBEDDEDLIST);
  }

  @Test(expected = YTSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkSet() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test5");

    db.executeInTx(
        () -> {
          YTDocument document = new YTDocument("Test5");
          Set<YTDocument> set = new HashSet<YTDocument>();
          set.add(new YTDocument("Test5"));
          document.field("somelinkset", set);
          db.save(document);
        });

    oClass.createProperty(db, "somelinkset", YTType.EMBEDDEDSET);
  }

  @Test(expected = YTSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddetSet() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test6");

    db.executeInTx(
        () -> {
          YTDocument document = new YTDocument("Test6");
          Set<YTDocument> list = new HashSet<YTDocument>();
          list.add(new YTDocument("Test6"));
          document.field("someembededset", list, YTType.EMBEDDEDSET);
          db.save(document);
        });

    oClass.createProperty(db, "someembededset", YTType.LINKSET);
  }

  @Test(expected = YTSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddedList() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test7");

    db.executeInTx(
        () -> {
          YTDocument document = new YTDocument("Test7");
          List<YTDocument> list = new ArrayList<YTDocument>();
          list.add(new YTDocument("Test7"));
          document.field("someembeddedlist", list, YTType.EMBEDDEDLIST);
          db.save(document);
        });

    oClass.createProperty(db, "someembeddedlist", YTType.LINKLIST);
  }

  @Test(expected = YTSchemaException.class)
  public void testCreatePropertyFailOnExistingDataEmbeddedMap() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test8");

    db.executeInTx(
        () -> {
          YTDocument document = new YTDocument("Test8");
          Map<String, YTDocument> map = new HashMap<>();
          map.put("test", new YTDocument("Test8"));
          document.field("someembededmap", map, YTType.EMBEDDEDMAP);
          db.save(document);
        });

    oClass.createProperty(db, "someembededmap", YTType.LINKMAP);
  }

  @Test(expected = YTSchemaException.class)
  public void testCreatePropertyFailOnExistingDataLinkMap() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test9");
    oSchema.createClass("Test8");

    db.executeInTx(
        () -> {
          YTDocument document = new YTDocument("Test9");
          Map<String, YTDocument> map = new HashMap<String, YTDocument>();
          map.put("test", new YTDocument("Test8"));
          document.field("somelinkmap", map, YTType.LINKMAP);
          db.save(document);
        });

    oClass.createProperty(db, "somelinkmap", YTType.EMBEDDEDMAP);
  }

  @Test
  public void testCreatePropertyCastable() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test10");

    var rid =
        db.computeInTx(
            () -> {
              YTDocument document = new YTDocument("Test10");
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

    oClass.createProperty(db, "test1", YTType.INTEGER);
    oClass.createProperty(db, "test2", YTType.LONG);
    oClass.createProperty(db, "test3", YTType.DOUBLE);
    oClass.createProperty(db, "test4", YTType.DOUBLE);
    oClass.createProperty(db, "test5", YTType.DECIMAL);
    oClass.createProperty(db, "test6", YTType.FLOAT);

    YTDocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1"), YTType.INTEGER);
    assertTrue(doc1.field("test1") instanceof Integer);
    assertEquals(doc1.fieldType("test2"), YTType.LONG);
    assertTrue(doc1.field("test2") instanceof Long);
    assertEquals(doc1.fieldType("test3"), YTType.DOUBLE);
    assertTrue(doc1.field("test3") instanceof Double);
    assertEquals(doc1.fieldType("test4"), YTType.DOUBLE);
    assertTrue(doc1.field("test4") instanceof Double);
    assertEquals(doc1.fieldType("test5"), YTType.DECIMAL);
    assertTrue(doc1.field("test5") instanceof BigDecimal);
    assertEquals(doc1.fieldType("test6"), YTType.FLOAT);
    assertTrue(doc1.field("test6") instanceof Float);
  }

  @Test
  public void testCreatePropertyCastableColection() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test11");

    var rid =
        db.computeInTx(
            () -> {
              YTDocument document = new YTDocument("Test11");
              document.field("test1", new ArrayList<YTDocument>(), YTType.EMBEDDEDLIST);
              document.field("test2", new ArrayList<YTDocument>(), YTType.LINKLIST);
              document.field("test3", new HashSet<YTDocument>(), YTType.EMBEDDEDSET);
              document.field("test4", new HashSet<YTDocument>(), YTType.LINKSET);
              document.field("test5", new HashMap<String, YTDocument>(), YTType.EMBEDDEDMAP);
              document.field("test6", new HashMap<String, YTDocument>(), YTType.LINKMAP);
              db.save(document);
              return document.getIdentity();
            });

    oClass.createProperty(db, "test1", YTType.LINKLIST);
    oClass.createProperty(db, "test2", YTType.EMBEDDEDLIST);
    oClass.createProperty(db, "test3", YTType.LINKSET);
    oClass.createProperty(db, "test4", YTType.EMBEDDEDSET);
    oClass.createProperty(db, "test5", YTType.LINKMAP);
    oClass.createProperty(db, "test6", YTType.EMBEDDEDMAP);

    YTDocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1"), YTType.LINKLIST);
    assertEquals(doc1.fieldType("test2"), YTType.EMBEDDEDLIST);
    assertEquals(doc1.fieldType("test3"), YTType.LINKSET);
    assertEquals(doc1.fieldType("test4"), YTType.EMBEDDEDSET);
    assertEquals(doc1.fieldType("test5"), YTType.LINKMAP);
    assertEquals(doc1.fieldType("test6"), YTType.EMBEDDEDMAP);
  }

  @Test
  public void testCreatePropertyIdKeep() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test12");
    YTProperty prop = oClass.createProperty(db, "test2", YTType.STRING);
    Integer id = prop.getId();
    oClass.dropProperty(db, "test2");
    prop = oClass.createProperty(db, "test2", YTType.STRING);
    assertEquals(id, prop.getId());
  }

  @Test
  public void testRenameProperty() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test13");
    YTProperty prop = oClass.createProperty(db, "test1", YTType.STRING);
    Integer id = prop.getId();
    prop.setName(db, "test2");
    assertNotEquals(id, prop.getId());
  }

  @Test
  public void testChangeTypeProperty() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test14");
    YTProperty prop = oClass.createProperty(db, "test1", YTType.SHORT);
    Integer id = prop.getId();
    prop.setType(db, YTType.INTEGER);
    assertNotEquals(id, prop.getId());
  }

  @Test
  public void testRenameBackProperty() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test15");
    YTProperty prop = oClass.createProperty(db, "test1", YTType.STRING);
    Integer id = prop.getId();
    prop.setName(db, "test2");
    assertNotEquals(id, prop.getId());
    prop.setName(db, "test1");
    assertEquals(id, prop.getId());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSetUncastableType() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test16");
    YTProperty prop = oClass.createProperty(db, "test1", YTType.STRING);
    prop.setType(db, YTType.INTEGER);
  }

  @Test
  public void testFindById() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test17");
    YTProperty prop = oClass.createProperty(db, "testaaa", YTType.STRING);
    OGlobalProperty global = oSchema.getGlobalPropertyById(prop.getId());

    assertEquals(prop.getId(), global.getId());
    assertEquals(prop.getName(), global.getName());
    assertEquals(prop.getType(), global.getType());
  }

  @Test
  public void testFindByIdDrop() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test18");
    YTProperty prop = oClass.createProperty(db, "testaaa", YTType.STRING);
    Integer id = prop.getId();
    oClass.dropProperty(db, "testaaa");
    OGlobalProperty global = oSchema.getGlobalPropertyById(id);

    assertEquals(id, global.getId());
    assertEquals("testaaa", global.getName());
    assertEquals(YTType.STRING, global.getType());
  }

  @Test
  public void testChangePropertyTypeCastable() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test19");

    oClass.createProperty(db, "test1", YTType.SHORT);
    oClass.createProperty(db, "test2", YTType.INTEGER);
    oClass.createProperty(db, "test3", YTType.LONG);
    oClass.createProperty(db, "test4", YTType.FLOAT);
    oClass.createProperty(db, "test5", YTType.DOUBLE);
    oClass.createProperty(db, "test6", YTType.INTEGER);

    var rid =
        db.computeInTx(
            () -> {
              YTDocument document = new YTDocument("Test19");
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

    oClass.getProperty("test1").setType(db, YTType.INTEGER);
    oClass.getProperty("test2").setType(db, YTType.LONG);
    oClass.getProperty("test3").setType(db, YTType.DOUBLE);
    oClass.getProperty("test4").setType(db, YTType.DOUBLE);
    oClass.getProperty("test5").setType(db, YTType.DECIMAL);
    oClass.getProperty("test6").setType(db, YTType.FLOAT);

    YTDocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1"), YTType.INTEGER);
    assertTrue(doc1.field("test1") instanceof Integer);
    assertEquals(doc1.fieldType("test2"), YTType.LONG);
    assertTrue(doc1.field("test2") instanceof Long);
    assertEquals(doc1.fieldType("test3"), YTType.DOUBLE);
    assertTrue(doc1.field("test3") instanceof Double);
    assertEquals(doc1.fieldType("test4"), YTType.DOUBLE);
    assertTrue(doc1.field("test4") instanceof Double);
    assertEquals(doc1.fieldType("test5"), YTType.DECIMAL);
    assertTrue(doc1.field("test5") instanceof BigDecimal);
    assertEquals(doc1.fieldType("test6"), YTType.FLOAT);
    assertTrue(doc1.field("test6") instanceof Float);
  }

  @Test
  public void testChangePropertyName() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test20");

    oClass.createProperty(db, "test1", YTType.SHORT);
    oClass.createProperty(db, "test2", YTType.INTEGER);
    oClass.createProperty(db, "test3", YTType.LONG);
    oClass.createProperty(db, "test4", YTType.FLOAT);
    oClass.createProperty(db, "test5", YTType.DOUBLE);
    oClass.createProperty(db, "test6", YTType.INTEGER);

    var rid =
        db.computeInTx(
            () -> {
              YTDocument document = new YTDocument("Test20");
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

    YTDocument doc1 = db.load(rid);
    assertEquals(doc1.fieldType("test1a"), YTType.SHORT);
    assertTrue(doc1.field("test1a") instanceof Short);
    assertEquals(doc1.fieldType("test2a"), YTType.INTEGER);
    assertTrue(doc1.field("test2a") instanceof Integer);
    assertEquals(doc1.fieldType("test3a"), YTType.LONG);
    assertTrue(doc1.field("test3a") instanceof Long);
    assertEquals(doc1.fieldType("test4a"), YTType.FLOAT);
    assertTrue(doc1.field("test4a") instanceof Float);
    assertEquals(doc1.fieldType("test5a"), YTType.DOUBLE);
    assertTrue(doc1.field("test5") instanceof Double);
    assertEquals(doc1.fieldType("test6a"), YTType.INTEGER);
    assertTrue(doc1.field("test6a") instanceof Integer);
  }

  @Test
  public void testCreatePropertyCastableColectionNoCache() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oClass = oSchema.createClass("Test11bis");

    var rid =
        db.computeInTx(
            () -> {
              final YTDocument document = new YTDocument("Test11bis");
              document.field("test1", new ArrayList<YTDocument>(), YTType.EMBEDDEDLIST);
              document.field("test2", new ArrayList<YTDocument>(), YTType.LINKLIST);

              document.field("test3", new HashSet<YTDocument>(), YTType.EMBEDDEDSET);
              document.field("test4", new HashSet<YTDocument>(), YTType.LINKSET);
              document.field("test5", new HashMap<String, YTDocument>(), YTType.EMBEDDEDMAP);
              document.field("test6", new HashMap<String, YTDocument>(), YTType.LINKMAP);
              db.save(document);

              return document.getIdentity();
            });
    oClass.createProperty(db, "test1", YTType.LINKLIST);
    oClass.createProperty(db, "test2", YTType.EMBEDDEDLIST);
    oClass.createProperty(db, "test3", YTType.LINKSET);
    oClass.createProperty(db, "test4", YTType.EMBEDDEDSET);
    oClass.createProperty(db, "test5", YTType.LINKMAP);
    oClass.createProperty(db, "test6", YTType.EMBEDDEDMAP);

    ExecutorService executor = Executors.newSingleThreadExecutor();

    Future<YTDocument> future =
        executor.submit(
            () -> {
              YTDocument doc1 = db.copy().load(rid);
              assertEquals(doc1.fieldType("test1"), YTType.LINKLIST);
              assertEquals(doc1.fieldType("test2"), YTType.EMBEDDEDLIST);
              assertEquals(doc1.fieldType("test3"), YTType.LINKSET);
              assertEquals(doc1.fieldType("test4"), YTType.EMBEDDEDSET);
              assertEquals(doc1.fieldType("test5"), YTType.LINKMAP);
              assertEquals(doc1.fieldType("test6"), YTType.EMBEDDEDMAP);
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

    final YTSchema oSchema = db.getMetadata().getSchema();
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
    final YTSchema oSchema = db.getMetadata().getSchema();

    YTClass oClass = oSchema.createClass(className);

    db.executeInTx(
        () -> {
          YTDocument record = db.newInstance(className);
          record.field("name", "foo");
          record.save();
        });

    oClass.createProperty(db, "name", YTType.ANY);

    try (YTResultSet result = db.query("select from " + className + " where name = 'foo'")) {
      assertEquals(result.stream().count(), 1);
    }
  }

  @Test
  public void testAlterCustomAttributeInClass() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("TestCreateCustomAttributeClass");

    oClass.setCustom(db, "customAttribute", "value1");
    assertEquals("value1", oClass.getCustom("customAttribute"));

    oClass.setCustom(db, "custom.attribute", "value2");
    assertEquals("value2", oClass.getCustom("custom.attribute"));
  }
}
