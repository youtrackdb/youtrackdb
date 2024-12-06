package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.CompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyListIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyMapIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyRidBagIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQLParsingException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ClassIndexTest extends DocumentDBBaseTest {

  private SchemaClass oClass;
  private SchemaClass oSuperClass;

  @Parameters(value = "remote")
  public ClassIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();

    oClass = schema.createClass("ClassIndexTestClass");
    oSuperClass = schema.createClass("ClassIndexTestSuperClass");

    oClass.createProperty(database, "fOne", PropertyType.INTEGER);
    oClass.createProperty(database, "fTwo", PropertyType.STRING);
    oClass.createProperty(database, "fThree", PropertyType.BOOLEAN);
    oClass.createProperty(database, "fFour", PropertyType.INTEGER);

    oClass.createProperty(database, "fSix", PropertyType.STRING);
    oClass.createProperty(database, "fSeven", PropertyType.STRING);

    oClass.createProperty(database, "fEight", PropertyType.INTEGER);
    oClass.createProperty(database, "fTen", PropertyType.INTEGER);
    oClass.createProperty(database, "fEleven", PropertyType.INTEGER);
    oClass.createProperty(database, "fTwelve", PropertyType.INTEGER);
    oClass.createProperty(database, "fThirteen", PropertyType.INTEGER);
    oClass.createProperty(database, "fFourteen", PropertyType.INTEGER);
    oClass.createProperty(database, "fFifteen", PropertyType.INTEGER);

    oClass.createProperty(database, "fEmbeddedMap", PropertyType.EMBEDDEDMAP, PropertyType.INTEGER);
    oClass.createProperty(database, "fEmbeddedMapWithoutLinkedType", PropertyType.EMBEDDEDMAP);
    oClass.createProperty(database, "fLinkMap", PropertyType.LINKMAP);

    oClass.createProperty(database, "fLinkList", PropertyType.LINKLIST);
    oClass.createProperty(database, "fEmbeddedList", PropertyType.EMBEDDEDLIST,
        PropertyType.INTEGER);

    oClass.createProperty(database, "fEmbeddedSet", PropertyType.EMBEDDEDSET, PropertyType.INTEGER);
    oClass.createProperty(database, "fLinkSet", PropertyType.LINKSET);

    oClass.createProperty(database, "fRidBag", PropertyType.LINKBAG);

    oSuperClass.createProperty(database, "fNine", PropertyType.INTEGER);
    oClass.setSuperClass(database, oSuperClass);

    database.close();
  }

  @Test
  public void testCreateOnePropertyIndexTest() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyOne",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fOne"});

    assertEquals(result.getName(), "ClassIndexTestPropertyOne");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestPropertyOne").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestPropertyOne")
            .getName(),
        result.getName());
  }

  @Test
  public void testCreateOnePropertyIndexInvalidName() {
    try {
      oClass.createIndex(database,
          "ClassIndex:TestPropertyOne",
          SchemaClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new EntityImpl().fields("ignoreNullValues", true), new String[]{"fOne"});
      fail();
    } catch (Exception e) {

      Throwable cause = e;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }

      assertTrue(
          (cause instanceof IllegalArgumentException)
              || (cause instanceof CommandSQLParsingException));
    }
  }

  @Test
  public void createCompositeIndexTestWithoutListener() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeOne",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fOne", "fTwo"});

    assertEquals(result.getName(), "ClassIndexTestCompositeOne");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestCompositeOne").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeOne")
            .getName(),
        result.getName());
  }

  @Test
  public void createCompositeIndexTestWithListener() {
    final AtomicInteger atomicInteger = new AtomicInteger(0);
    final ProgressListener progressListener =
        new ProgressListener() {
          @Override
          public void onBegin(final Object iTask, final long iTotal, Object metadata) {
            atomicInteger.incrementAndGet();
          }

          @Override
          public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
            return true;
          }

          @Override
          public void onCompletition(DatabaseSessionInternal session, final Object iTask,
              final boolean iSucceed) {
            atomicInteger.incrementAndGet();
          }
        };

    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeTwo",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            progressListener,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fOne", "fTwo", "fThree"});

    assertEquals(result.getName(), "ClassIndexTestCompositeTwo");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestCompositeTwo").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeTwo")
            .getName(),
        result.getName());
    assertEquals(atomicInteger.get(), 2);
  }

  @Test
  public void testCreateOnePropertyEmbeddedMapIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyEmbeddedMap",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fEmbeddedMap"});

    assertEquals(result.getName(), "ClassIndexTestPropertyEmbeddedMap");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestPropertyEmbeddedMap").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestPropertyEmbeddedMap")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], PropertyType.STRING);
    assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateCompositeEmbeddedMapIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedMap",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fFifteen", "fEmbeddedMap"});

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedMap");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeEmbeddedMap").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedMap")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fFifteen", "fEmbeddedMap"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.STRING});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedMapByKeyIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedMapByKey",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fEight", "fEmbeddedMap"});

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedMapByKey");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeEmbeddedMapByKey").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(
                database, "ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedMapByKey")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fEight", "fEmbeddedMap"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.STRING});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedMapByValueIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedMapByValue",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fTen", "fEmbeddedMap by value"});

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedMapByValue");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeEmbeddedMapByValue").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(
                database, "ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedMapByValue")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fTen", "fEmbeddedMap"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.INTEGER});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeLinkMapByValueIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeLinkMapByValue",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fEleven", "fLinkMap by value"});

    assertEquals(result.getName(), "ClassIndexTestCompositeLinkMapByValue");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeLinkMapByValue").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeLinkMapByValue")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fEleven", "fLinkMap"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedSetIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedSet",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fTwelve", "fEmbeddedSet"});

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedSet");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeEmbeddedSet").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedSet")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fTwelve", "fEmbeddedSet"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.INTEGER});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test(dependsOnMethods = "testGetIndexes")
  public void testCreateCompositeLinkSetIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeLinkSet",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fTwelve", "fLinkSet"});

    assertEquals(result.getName(), "ClassIndexTestCompositeLinkSet");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeLinkSet").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeLinkSet")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fTwelve", "fLinkSet"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedListIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedList",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fThirteen", "fEmbeddedList"});

    assertEquals(result.getName(), "ClassIndexTestCompositeEmbeddedList");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeEmbeddedList").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeEmbeddedList")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(
        indexDefinition.getFields().toArray(), new String[]{"fThirteen", "fEmbeddedList"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.INTEGER});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  public void testCreateCompositeLinkListIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeLinkList",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fFourteen", "fLinkList"});

    assertEquals(result.getName(), "ClassIndexTestCompositeLinkList");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestCompositeLinkList").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeLinkList")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fFourteen", "fLinkList"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  public void testCreateCompositeRidBagIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeRidBag",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fFourteen", "fRidBag"});

    assertEquals(result.getName(), "ClassIndexTestCompositeRidBag");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestCompositeRidBag").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestCompositeRidBag")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof CompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fFourteen", "fRidBag"});

    assertEquals(indexDefinition.getTypes(),
        new PropertyType[]{PropertyType.INTEGER, PropertyType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateOnePropertyLinkedMapIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyLinkedMap",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fLinkMap"});

    assertEquals(result.getName(), "ClassIndexTestPropertyLinkedMap");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestPropertyLinkedMap").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestPropertyLinkedMap")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], PropertyType.STRING);
    assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyLinkMapByKeyIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyLinkedMapByKey",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fLinkMap by key"});

    assertEquals(result.getName(), "ClassIndexTestPropertyLinkedMapByKey");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestPropertyLinkedMapByKey").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, "ClassIndexTestClass", "ClassIndexTestPropertyLinkedMapByKey")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], PropertyType.STRING);
    assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyLinkMapByValueIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyLinkedMapByValue",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fLinkMap by value"});

    assertEquals(result.getName(), "ClassIndexTestPropertyLinkedMapByValue");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestPropertyLinkedMapByValue").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(
                database, "ClassIndexTestClass", "ClassIndexTestPropertyLinkedMapByValue")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], PropertyType.LINK);
    assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateOnePropertyByKeyEmbeddedMapIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyByKeyEmbeddedMap",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fEmbeddedMap by key"});

    assertEquals(result.getName(), "ClassIndexTestPropertyByKeyEmbeddedMap");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestPropertyByKeyEmbeddedMap").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(
                database, "ClassIndexTestClass", "ClassIndexTestPropertyByKeyEmbeddedMap")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], PropertyType.STRING);
    assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyByValueEmbeddedMapIndex() {
    final Index result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyByValueEmbeddedMap",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true),
            new String[]{"fEmbeddedMap by value"});

    assertEquals(result.getName(), "ClassIndexTestPropertyByValueEmbeddedMap");
    assertEquals(
        oClass.getClassIndex(database, "ClassIndexTestPropertyByValueEmbeddedMap").getName(),
        result.getName());
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(
                database, "ClassIndexTestClass", "ClassIndexTestPropertyByValueEmbeddedMap")
            .getName(),
        result.getName());

    final IndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof PropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], PropertyType.INTEGER);
    assertEquals(
        ((PropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        PropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexOne() {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex(database,
          "ClassIndexTestPropertyWrongSpecifierEmbeddedMap",
          SchemaClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new EntityImpl().fields("ignoreNullValues", true), new String[]{"fEmbeddedMap by ttt"});
    } catch (Exception e) {
      Assert.assertTrue(e instanceof IllegalArgumentException);
      exceptionIsThrown = true;
      assertEquals(
          e.getMessage(),
          "Illegal field name format, should be '<property> [by key|value]' but was 'fEmbeddedMap"
              + " by ttt'");
    }

    assertTrue(exceptionIsThrown);
    assertNull(oClass.getClassIndex(database, "ClassIndexTestPropertyWrongSpecifierEmbeddedMap"));
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexTwo() {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex(database,
          "ClassIndexTestPropertyWrongSpecifierEmbeddedMap",
          SchemaClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new EntityImpl().fields("ignoreNullValues", true),
          new String[]{"fEmbeddedMap b value"});
    } catch (IndexException e) {
      exceptionIsThrown = true;
    }

    assertTrue(exceptionIsThrown);
    assertNull(oClass.getClassIndex(database, "ClassIndexTestPropertyWrongSpecifierEmbeddedMap"));
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexThree() {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex(database,
          "ClassIndexTestPropertyWrongSpecifierEmbeddedMap",
          SchemaClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new EntityImpl().fields("ignoreNullValues", true),
          new String[]{"fEmbeddedMap by value t"});
    } catch (IndexException e) {
      exceptionIsThrown = true;
    }

    assertTrue(exceptionIsThrown);
    assertNull(oClass.getClassIndex(database, "ClassIndexTestPropertyWrongSpecifierEmbeddedMap"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedOneProperty() {
    final boolean result = oClass.areIndexed(database, List.of("fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedEightProperty() {
    final boolean result = oClass.areIndexed(database, List.of("fEight"));
    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedEightPropertyEmbeddedMap() {
    final boolean result = oClass.areIndexed(database, Arrays.asList("fEmbeddedMap", "fEight"));
    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedDoesNotContainProperty() {
    final boolean result = oClass.areIndexed(database, List.of("fSix"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedTwoProperties() {
    final boolean result = oClass.areIndexed(database, Arrays.asList("fTwo", "fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedThreeProperties() {
    final boolean result = oClass.areIndexed(database, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedPropertiesNotFirst() {
    final boolean result = oClass.areIndexed(database, Arrays.asList("fTwo", "fTree"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedPropertiesMoreThanNeeded() {
    final boolean result = oClass.areIndexed(database,
        Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "createParentPropertyIndex",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedParentProperty() {
    final boolean result = oClass.areIndexed(database, List.of("fNine"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedParentChildProperty() {
    final boolean result = oClass.areIndexed(database, List.of("fOne, fNine"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedOnePropertyArrayParams() {
    final boolean result = oClass.areIndexed(database, "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedDoesNotContainPropertyArrayParams() {
    final boolean result = oClass.areIndexed(database, "fSix");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedTwoPropertiesArrayParams() {
    final boolean result = oClass.areIndexed(database, "fTwo", "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedThreePropertiesArrayParams() {
    final boolean result = oClass.areIndexed(database, "fTwo", "fOne", "fThree");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedPropertiesNotFirstArrayParams() {
    final boolean result = oClass.areIndexed(database, "fTwo", "fTree");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
    final boolean result = oClass.areIndexed(database, "fTwo", "fOne", "fThee", "fFour");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "createParentPropertyIndex",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedParentPropertyArrayParams() {
    final boolean result = oClass.areIndexed(database, "fNine");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testAreIndexedParentChildPropertyArrayParams() {
    final boolean result = oClass.areIndexed(database, "fOne, fNine");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database, "fOne");

    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesTwoPropertiesArrayParams() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fOne");
    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesThreePropertiesArrayParams() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fOne", "fThree");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fOne", "fThee",
        "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
    final Set<Index> result =
        oClass.getClassInvolvedIndexes(database, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesOneProperty() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database, List.of("fOne"));

    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesTwoProperties() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database,
        Arrays.asList("fTwo", "fOne"));
    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesThreeProperties() {
    final Set<Index> result =
        oClass.getClassInvolvedIndexes(database, Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesNotInvolvedProperties() {
    final Set<Index> result = oClass.getClassInvolvedIndexes(database,
        Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
    final Set<Index> result =
        oClass.getClassInvolvedIndexes(database, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesOnePropertyArrayParams() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, "fOne");

    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesTwoPropertiesArrayParams() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, "fTwo", "fOne");
    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesThreePropertiesArrayParams() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, "fTwo", "fOne", "fThree");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesNotInvolvedPropertiesArrayParams() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, "fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetParentInvolvedIndexesArrayParams() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, "fNine");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestParentPropertyNine");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetParentChildInvolvedIndexesArrayParams() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, "fOne", "fNine");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesOneProperty() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, List.of("fOne"));

    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestPropertyOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesTwoProperties() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, Arrays.asList("fTwo", "fOne"));
    assertEquals(result.size(), 1);

    assertTrue(containsIndex(result, "ClassIndexTestCompositeOne"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesThreeProperties() {
    final Set<Index> result = oClass.getInvolvedIndexes(database,
        Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestCompositeTwo");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetInvolvedIndexesNotInvolvedProperties() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetParentInvolvedIndexes() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, List.of("fNine"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "ClassIndexTestParentPropertyNine");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex"
      })
  public void testGetParentChildInvolvedIndexes() {
    final Set<Index> result = oClass.getInvolvedIndexes(database, Arrays.asList("fOne", "fNine"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex",
          "testCreateCompositeLinkListIndex",
          "testCreateCompositeRidBagIndex"
      })
  public void testGetClassIndexes() {
    final Set<Index> indexes = oClass.getClassIndexes(database);
    final Set<IndexDefinition> expectedIndexDefinitions = new HashSet<>();

    final CompositeIndexDefinition compositeIndexOne =
        new CompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexOne.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fOne", PropertyType.INTEGER));
    compositeIndexOne.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTwo", PropertyType.STRING));
    expectedIndexDefinitions.add(compositeIndexOne);

    final CompositeIndexDefinition compositeIndexTwo =
        new CompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fOne", PropertyType.INTEGER));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTwo", PropertyType.STRING));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fThree", PropertyType.BOOLEAN));
    expectedIndexDefinitions.add(compositeIndexTwo);

    final CompositeIndexDefinition compositeIndexThree =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexThree.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fEight", PropertyType.INTEGER));
    compositeIndexThree.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexThree);

    final CompositeIndexDefinition compositeIndexFour =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFour.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTen", PropertyType.INTEGER));
    compositeIndexFour.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.INTEGER,
            PropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFour);

    final CompositeIndexDefinition compositeIndexFive =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFive.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fEleven", PropertyType.INTEGER));
    compositeIndexFive.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            PropertyType.LINK,
            PropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFive);

    final CompositeIndexDefinition compositeIndexSix =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSix.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTwelve", PropertyType.INTEGER));
    compositeIndexSix.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedSet",
            PropertyType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSix);

    final CompositeIndexDefinition compositeIndexSeven =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSeven.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fThirteen", PropertyType.INTEGER));
    compositeIndexSeven.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList",
            PropertyType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSeven);

    final CompositeIndexDefinition compositeIndexEight =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEight.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFourteen", PropertyType.INTEGER));
    compositeIndexEight.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", PropertyType.LINK));
    expectedIndexDefinitions.add(compositeIndexEight);

    final CompositeIndexDefinition compositeIndexNine =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexNine.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFifteen", PropertyType.INTEGER));
    compositeIndexNine.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexNine);

    final CompositeIndexDefinition compositeIndexTen =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexTen.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFourteen", PropertyType.INTEGER));
    compositeIndexTen.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fLinkList", PropertyType.LINK));
    expectedIndexDefinitions.add(compositeIndexTen);

    final CompositeIndexDefinition compositeIndexEleven =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEleven.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFourteen", PropertyType.INTEGER));
    compositeIndexEleven.addIndex(
        new PropertyRidBagIndexDefinition("ClassIndexTestClass", "fRidBag"));
    expectedIndexDefinitions.add(compositeIndexEleven);

    final PropertyIndexDefinition propertyIndex =
        new PropertyIndexDefinition("ClassIndexTestClass", "fOne", PropertyType.INTEGER);
    expectedIndexDefinitions.add(propertyIndex);

    final PropertyMapIndexDefinition propertyMapIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyMapIndexDefinition);

    final PropertyMapIndexDefinition propertyMapByValueIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.INTEGER,
            PropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyMapByValueIndexDefinition);

    final PropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyLinkMapByKeyIndexDefinition);

    final PropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            PropertyType.LINK,
            PropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyLinkMapByValueIndexDefinition);

    assertEquals(indexes.size(), 17);

    for (final Index index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest",
          "createParentPropertyIndex",
          "testCreateOnePropertyEmbeddedMapIndex",
          "testCreateOnePropertyByKeyEmbeddedMapIndex",
          "testCreateOnePropertyByValueEmbeddedMapIndex",
          "testCreateOnePropertyLinkedMapIndex",
          "testCreateOnePropertyLinkMapByKeyIndex",
          "testCreateOnePropertyLinkMapByValueIndex",
          "testCreateCompositeEmbeddedMapIndex",
          "testCreateCompositeEmbeddedMapByKeyIndex",
          "testCreateCompositeEmbeddedMapByValueIndex",
          "testCreateCompositeLinkMapByValueIndex",
          "testCreateCompositeEmbeddedSetIndex",
          "testCreateCompositeEmbeddedListIndex",
          "testCreateCompositeLinkListIndex",
          "testCreateCompositeRidBagIndex"
      })
  public void testGetIndexes() {
    final Set<Index> indexes = oClass.getIndexes(database);
    final Set<IndexDefinition> expectedIndexDefinitions = new HashSet<>();

    final CompositeIndexDefinition compositeIndexOne =
        new CompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexOne.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fOne", PropertyType.INTEGER));
    compositeIndexOne.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTwo", PropertyType.STRING));
    expectedIndexDefinitions.add(compositeIndexOne);

    final CompositeIndexDefinition compositeIndexTwo =
        new CompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fOne", PropertyType.INTEGER));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTwo", PropertyType.STRING));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fThree", PropertyType.BOOLEAN));
    expectedIndexDefinitions.add(compositeIndexTwo);

    final CompositeIndexDefinition compositeIndexThree =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexThree.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fEight", PropertyType.INTEGER));
    compositeIndexThree.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexThree);

    final CompositeIndexDefinition compositeIndexFour =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFour.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTen", PropertyType.INTEGER));
    compositeIndexFour.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.INTEGER,
            PropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFour);

    final CompositeIndexDefinition compositeIndexFive =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFive.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fEleven", PropertyType.INTEGER));
    compositeIndexFive.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            PropertyType.LINK,
            PropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFive);

    final CompositeIndexDefinition compositeIndexSix =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSix.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fTwelve", PropertyType.INTEGER));
    compositeIndexSix.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedSet",
            PropertyType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSix);

    final CompositeIndexDefinition compositeIndexSeven =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSeven.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fThirteen", PropertyType.INTEGER));
    compositeIndexSeven.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList",
            PropertyType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSeven);

    final CompositeIndexDefinition compositeIndexEight =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEight.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFourteen", PropertyType.INTEGER));
    compositeIndexEight.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", PropertyType.LINK));
    expectedIndexDefinitions.add(compositeIndexEight);

    final CompositeIndexDefinition compositeIndexNine =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexNine.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFifteen", PropertyType.INTEGER));
    compositeIndexNine.addIndex(
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexNine);

    final CompositeIndexDefinition compositeIndexTen =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexTen.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFourteen", PropertyType.INTEGER));
    compositeIndexTen.addIndex(
        new PropertyListIndexDefinition("ClassIndexTestClass", "fLinkList", PropertyType.LINK));
    expectedIndexDefinitions.add(compositeIndexTen);

    final CompositeIndexDefinition compositeIndexEleven =
        new CompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEleven.addIndex(
        new PropertyIndexDefinition("ClassIndexTestClass", "fFourteen", PropertyType.INTEGER));
    compositeIndexEleven.addIndex(
        new PropertyRidBagIndexDefinition("ClassIndexTestClass", "fRidBag"));
    expectedIndexDefinitions.add(compositeIndexEleven);

    final PropertyIndexDefinition propertyIndex =
        new PropertyIndexDefinition("ClassIndexTestClass", "fOne", PropertyType.INTEGER);
    expectedIndexDefinitions.add(propertyIndex);

    final PropertyIndexDefinition parentPropertyIndex =
        new PropertyIndexDefinition("ClassIndexTestSuperClass", "fNine", PropertyType.INTEGER);
    expectedIndexDefinitions.add(parentPropertyIndex);

    final PropertyMapIndexDefinition propertyMapIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyMapIndexDefinition);

    final PropertyMapIndexDefinition propertyMapByValueIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            PropertyType.INTEGER,
            PropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyMapByValueIndexDefinition);

    final PropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            PropertyType.STRING,
            PropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyLinkMapByKeyIndexDefinition);

    final PropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition =
        new PropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            PropertyType.LINK,
            PropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyLinkMapByValueIndexDefinition);

    assertEquals(indexes.size(), 18);

    for (final Index index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test
  public void testGetIndexesWithoutParent() {
    final SchemaClass inClass = database.getMetadata().getSchema().createClass("ClassIndexInTest");
    inClass.createProperty(database, "fOne", PropertyType.INTEGER);

    final Index result =
        inClass.createIndex(database,
            "ClassIndexInTestPropertyOne",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fOne"});

    assertEquals(result.getName(), "ClassIndexInTestPropertyOne");
    assertEquals(inClass.getClassIndex(database, "ClassIndexInTestPropertyOne").getName(),
        result.getName());

    final Set<Index> indexes = inClass.getIndexes(database);
    final PropertyIndexDefinition propertyIndexDefinition =
        new PropertyIndexDefinition("ClassIndexInTest", "fOne", PropertyType.INTEGER);

    assertEquals(indexes.size(), 1);

    assertEquals(propertyIndexDefinition, indexes.iterator().next().getDefinition());
  }

  @Test(expectedExceptions = IndexException.class)
  public void testCreateIndexEmptyFields() {
    oClass.createIndex(database, "ClassIndexTestCompositeEmpty", SchemaClass.INDEX_TYPE.UNIQUE);
  }

  @Test(expectedExceptions = IndexException.class)
  public void testCreateIndexAbsentFields() {
    oClass.createIndex(database,
        "ClassIndexTestCompositeFieldAbsent",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"fFive"});
  }

  @Test
  public void testCreateProxyIndex() {
    try {
      oClass.createIndex(database, "ClassIndexTestProxyIndex", SchemaClass.INDEX_TYPE.PROXY,
          "fOne");
      Assert.fail();
    } catch (IndexException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "testGetInvolvedIndexesOnePropertyArrayParams")
  public void testCreateDictionaryIndex() {
    final Index result =
        oClass.createIndex(database, "ClassIndexTestDictionaryIndex",
            SchemaClass.INDEX_TYPE.DICTIONARY,
            "fOne");

    assertEquals(result.getName(), "ClassIndexTestDictionaryIndex");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestDictionaryIndex").getName(),
        result.getName());
    assertEquals(result.getType(), SchemaClass.INDEX_TYPE.DICTIONARY.toString());
  }

  @Test(dependsOnMethods = "testGetInvolvedIndexesOnePropertyArrayParams")
  public void testCreateNotUniqueIndex() {
    final Index result =
        oClass.createIndex(database, "ClassIndexTestNotUniqueIndex",
            SchemaClass.INDEX_TYPE.NOTUNIQUE,
            "fOne");

    assertEquals(result.getName(), "ClassIndexTestNotUniqueIndex");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestNotUniqueIndex").getName(),
        result.getName());
    assertEquals(result.getType(), SchemaClass.INDEX_TYPE.NOTUNIQUE.toString());
  }

  @Test
  public void testCreateMapWithoutLinkedType() {
    try {
      oClass.createIndex(database,
          "ClassIndexMapWithoutLinkedTypeIndex",
          SchemaClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedMapWithoutLinkedType by value");
      fail();
    } catch (IndexException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. You should provide linked type for embedded"
                      + " collections that are going to be indexed."));
    }
  }

  public void createParentPropertyIndex() {
    final Index result =
        oSuperClass.createIndex(database,
            "ClassIndexTestParentPropertyNine",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new EntityImpl().fields("ignoreNullValues", true), new String[]{"fNine"});

    assertEquals(result.getName(), "ClassIndexTestParentPropertyNine");
    assertEquals(
        oSuperClass.getClassIndex(database, "ClassIndexTestParentPropertyNine").getName(),
        result.getName());
  }

  private boolean containsIndex(
      final Collection<? extends Index> classIndexes, final String indexName) {
    for (final Index index : classIndexes) {
      if (index.getName().equals(indexName)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testDropProperty() throws Exception {
    oClass.createProperty(database, "fFive", PropertyType.INTEGER);

    oClass.dropProperty(database, "fFive");

    assertNull(oClass.getProperty("fFive"));
  }
}
