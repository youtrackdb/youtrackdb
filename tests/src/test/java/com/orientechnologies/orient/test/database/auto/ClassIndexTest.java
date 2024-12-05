package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.orientechnologies.common.listener.OProgressListener;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyListIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyMapIndexDefinition;
import com.orientechnologies.orient.core.index.OPropertyRidBagIndexDefinition;
import com.orientechnologies.orient.core.index.YTIndexException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.YTCommandSQLParsingException;
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

  private YTClass oClass;
  private YTClass oSuperClass;

  @Parameters(value = "remote")
  public ClassIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final YTSchema schema = database.getMetadata().getSchema();

    oClass = schema.createClass("ClassIndexTestClass");
    oSuperClass = schema.createClass("ClassIndexTestSuperClass");

    oClass.createProperty(database, "fOne", YTType.INTEGER);
    oClass.createProperty(database, "fTwo", YTType.STRING);
    oClass.createProperty(database, "fThree", YTType.BOOLEAN);
    oClass.createProperty(database, "fFour", YTType.INTEGER);

    oClass.createProperty(database, "fSix", YTType.STRING);
    oClass.createProperty(database, "fSeven", YTType.STRING);

    oClass.createProperty(database, "fEight", YTType.INTEGER);
    oClass.createProperty(database, "fTen", YTType.INTEGER);
    oClass.createProperty(database, "fEleven", YTType.INTEGER);
    oClass.createProperty(database, "fTwelve", YTType.INTEGER);
    oClass.createProperty(database, "fThirteen", YTType.INTEGER);
    oClass.createProperty(database, "fFourteen", YTType.INTEGER);
    oClass.createProperty(database, "fFifteen", YTType.INTEGER);

    oClass.createProperty(database, "fEmbeddedMap", YTType.EMBEDDEDMAP, YTType.INTEGER);
    oClass.createProperty(database, "fEmbeddedMapWithoutLinkedType", YTType.EMBEDDEDMAP);
    oClass.createProperty(database, "fLinkMap", YTType.LINKMAP);

    oClass.createProperty(database, "fLinkList", YTType.LINKLIST);
    oClass.createProperty(database, "fEmbeddedList", YTType.EMBEDDEDLIST, YTType.INTEGER);

    oClass.createProperty(database, "fEmbeddedSet", YTType.EMBEDDEDSET, YTType.INTEGER);
    oClass.createProperty(database, "fLinkSet", YTType.LINKSET);

    oClass.createProperty(database, "fRidBag", YTType.LINKBAG);

    oSuperClass.createProperty(database, "fNine", YTType.INTEGER);
    oClass.setSuperClass(database, oSuperClass);

    database.close();
  }

  @Test
  public void testCreateOnePropertyIndexTest() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyOne",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fOne"});

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
          YTClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fOne"});
      fail();
    } catch (Exception e) {

      Throwable cause = e;
      while (cause.getCause() != null) {
        cause = cause.getCause();
      }

      assertTrue(
          (cause instanceof IllegalArgumentException)
              || (cause instanceof YTCommandSQLParsingException));
    }
  }

  @Test
  public void createCompositeIndexTestWithoutListener() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeOne",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fOne", "fTwo"});

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
    final OProgressListener progressListener =
        new OProgressListener() {
          @Override
          public void onBegin(final Object iTask, final long iTotal, Object metadata) {
            atomicInteger.incrementAndGet();
          }

          @Override
          public boolean onProgress(final Object iTask, final long iCounter, final float iPercent) {
            return true;
          }

          @Override
          public void onCompletition(YTDatabaseSessionInternal session, final Object iTask,
              final boolean iSucceed) {
            atomicInteger.incrementAndGet();
          }
        };

    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeTwo",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            progressListener,
            new YTEntityImpl().fields("ignoreNullValues", true),
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
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyEmbeddedMap",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fEmbeddedMap"});

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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], YTType.STRING);
    assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateCompositeEmbeddedMapIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedMap",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fFifteen", "fEmbeddedMap"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.STRING});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedMapByKeyIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedMapByKey",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fEight", "fEmbeddedMap"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.STRING});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedMapByValueIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedMapByValue",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fTen", "fEmbeddedMap"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.INTEGER});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeLinkMapByValueIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeLinkMapByValue",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fEleven", "fLinkMap"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedSetIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedSet",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fTwelve", "fEmbeddedSet"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.INTEGER});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test(dependsOnMethods = "testGetIndexes")
  public void testCreateCompositeLinkSetIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeLinkSet",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fTwelve", "fLinkSet"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateCompositeEmbeddedListIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeEmbeddedList",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(
        indexDefinition.getFields().toArray(), new String[]{"fThirteen", "fEmbeddedList"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.INTEGER});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  public void testCreateCompositeLinkListIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeLinkList",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fFourteen", "fLinkList"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  public void testCreateCompositeRidBagIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestCompositeRidBag",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    assertEquals(indexDefinition.getFields().toArray(), new String[]{"fFourteen", "fRidBag"});

    assertEquals(indexDefinition.getTypes(), new YTType[]{YTType.INTEGER, YTType.LINK});
    assertEquals(indexDefinition.getParamCount(), 2);
  }

  @Test
  public void testCreateOnePropertyLinkedMapIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyLinkedMap",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fLinkMap"});

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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], YTType.STRING);
    assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyLinkMapByKeyIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyLinkedMapByKey",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fLinkMap by key"});

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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], YTType.STRING);
    assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyLinkMapByValueIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyLinkedMapByValue",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fLinkMap by value"});

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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fLinkMap");
    assertEquals(indexDefinition.getTypes()[0], YTType.LINK);
    assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateOnePropertyByKeyEmbeddedMapIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyByKeyEmbeddedMap",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], YTType.STRING);
    assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.KEY);
  }

  @Test
  public void testCreateOnePropertyByValueEmbeddedMapIndex() {
    final OIndex result =
        oClass.createIndex(database,
            "ClassIndexTestPropertyByValueEmbeddedMap",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true),
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

    final OIndexDefinition indexDefinition = result.getDefinition();

    assertTrue(indexDefinition instanceof OPropertyMapIndexDefinition);
    assertEquals(indexDefinition.getFields().get(0), "fEmbeddedMap");
    assertEquals(indexDefinition.getTypes()[0], YTType.INTEGER);
    assertEquals(
        ((OPropertyMapIndexDefinition) indexDefinition).getIndexBy(),
        OPropertyMapIndexDefinition.INDEX_BY.VALUE);
  }

  @Test
  public void testCreateOnePropertyWrongSpecifierEmbeddedMapIndexOne() {
    boolean exceptionIsThrown = false;
    try {
      oClass.createIndex(database,
          "ClassIndexTestPropertyWrongSpecifierEmbeddedMap",
          YTClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fEmbeddedMap by ttt"});
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
          YTClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new YTEntityImpl().fields("ignoreNullValues", true),
          new String[]{"fEmbeddedMap b value"});
    } catch (YTIndexException e) {
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
          YTClass.INDEX_TYPE.UNIQUE.toString(),
          null,
          new YTEntityImpl().fields("ignoreNullValues", true),
          new String[]{"fEmbeddedMap by value t"});
    } catch (YTIndexException e) {
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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database, "fOne");

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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fOne");
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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fOne", "fThree");

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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fFour");

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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database, "fTwo", "fOne", "fThee",
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
    final Set<OIndex> result =
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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database, List.of("fOne"));

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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database,
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
    final Set<OIndex> result =
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
    final Set<OIndex> result = oClass.getClassInvolvedIndexes(database,
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
    final Set<OIndex> result =
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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, "fOne");

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, "fTwo", "fOne");
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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, "fTwo", "fOne", "fThree");

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, "fTwo", "fFour");

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, "fNine");

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, "fOne", "fNine");

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, List.of("fOne"));

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, Arrays.asList("fTwo", "fOne"));
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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database,
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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, Arrays.asList("fTwo", "fFour"));

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, List.of("fNine"));

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
    final Set<OIndex> result = oClass.getInvolvedIndexes(database, Arrays.asList("fOne", "fNine"));

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
    final Set<OIndex> indexes = oClass.getClassIndexes(database);
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<>();

    final OCompositeIndexDefinition compositeIndexOne =
        new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexOne.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", YTType.INTEGER));
    compositeIndexOne.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", YTType.STRING));
    expectedIndexDefinitions.add(compositeIndexOne);

    final OCompositeIndexDefinition compositeIndexTwo =
        new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", YTType.INTEGER));
    compositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", YTType.STRING));
    compositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fThree", YTType.BOOLEAN));
    expectedIndexDefinitions.add(compositeIndexTwo);

    final OCompositeIndexDefinition compositeIndexThree =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexThree.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fEight", YTType.INTEGER));
    compositeIndexThree.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexThree);

    final OCompositeIndexDefinition compositeIndexFour =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFour.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTen", YTType.INTEGER));
    compositeIndexFour.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.INTEGER,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFour);

    final OCompositeIndexDefinition compositeIndexFive =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFive.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fEleven", YTType.INTEGER));
    compositeIndexFive.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            YTType.LINK,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFive);

    final OCompositeIndexDefinition compositeIndexSix =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSix.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTwelve", YTType.INTEGER));
    compositeIndexSix.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedSet", YTType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSix);

    final OCompositeIndexDefinition compositeIndexSeven =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSeven.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fThirteen", YTType.INTEGER));
    compositeIndexSeven.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", YTType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSeven);

    final OCompositeIndexDefinition compositeIndexEight =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEight.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", YTType.INTEGER));
    compositeIndexEight.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", YTType.LINK));
    expectedIndexDefinitions.add(compositeIndexEight);

    final OCompositeIndexDefinition compositeIndexNine =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexNine.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFifteen", YTType.INTEGER));
    compositeIndexNine.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexNine);

    final OCompositeIndexDefinition compositeIndexTen =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexTen.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", YTType.INTEGER));
    compositeIndexTen.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fLinkList", YTType.LINK));
    expectedIndexDefinitions.add(compositeIndexTen);

    final OCompositeIndexDefinition compositeIndexEleven =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEleven.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", YTType.INTEGER));
    compositeIndexEleven.addIndex(
        new OPropertyRidBagIndexDefinition("ClassIndexTestClass", "fRidBag"));
    expectedIndexDefinitions.add(compositeIndexEleven);

    final OPropertyIndexDefinition propertyIndex =
        new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", YTType.INTEGER);
    expectedIndexDefinitions.add(propertyIndex);

    final OPropertyMapIndexDefinition propertyMapIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyMapIndexDefinition);

    final OPropertyMapIndexDefinition propertyMapByValueIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.INTEGER,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyMapByValueIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyLinkMapByKeyIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            YTType.LINK,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyLinkMapByValueIndexDefinition);

    assertEquals(indexes.size(), 17);

    for (final OIndex index : indexes) {
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
    final Set<OIndex> indexes = oClass.getIndexes(database);
    final Set<OIndexDefinition> expectedIndexDefinitions = new HashSet<>();

    final OCompositeIndexDefinition compositeIndexOne =
        new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexOne.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", YTType.INTEGER));
    compositeIndexOne.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", YTType.STRING));
    expectedIndexDefinitions.add(compositeIndexOne);

    final OCompositeIndexDefinition compositeIndexTwo =
        new OCompositeIndexDefinition("ClassIndexTestClass");

    compositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", YTType.INTEGER));
    compositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTwo", YTType.STRING));
    compositeIndexTwo.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fThree", YTType.BOOLEAN));
    expectedIndexDefinitions.add(compositeIndexTwo);

    final OCompositeIndexDefinition compositeIndexThree =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexThree.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fEight", YTType.INTEGER));
    compositeIndexThree.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexThree);

    final OCompositeIndexDefinition compositeIndexFour =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFour.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTen", YTType.INTEGER));
    compositeIndexFour.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.INTEGER,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFour);

    final OCompositeIndexDefinition compositeIndexFive =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexFive.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fEleven", YTType.INTEGER));
    compositeIndexFive.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            YTType.LINK,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE));
    expectedIndexDefinitions.add(compositeIndexFive);

    final OCompositeIndexDefinition compositeIndexSix =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSix.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fTwelve", YTType.INTEGER));
    compositeIndexSix.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedSet", YTType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSix);

    final OCompositeIndexDefinition compositeIndexSeven =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexSeven.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fThirteen", YTType.INTEGER));
    compositeIndexSeven.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", YTType.INTEGER));
    expectedIndexDefinitions.add(compositeIndexSeven);

    final OCompositeIndexDefinition compositeIndexEight =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEight.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", YTType.INTEGER));
    compositeIndexEight.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fEmbeddedList", YTType.LINK));
    expectedIndexDefinitions.add(compositeIndexEight);

    final OCompositeIndexDefinition compositeIndexNine =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexNine.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFifteen", YTType.INTEGER));
    compositeIndexNine.addIndex(
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY));
    expectedIndexDefinitions.add(compositeIndexNine);

    final OCompositeIndexDefinition compositeIndexTen =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexTen.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", YTType.INTEGER));
    compositeIndexTen.addIndex(
        new OPropertyListIndexDefinition("ClassIndexTestClass", "fLinkList", YTType.LINK));
    expectedIndexDefinitions.add(compositeIndexTen);

    final OCompositeIndexDefinition compositeIndexEleven =
        new OCompositeIndexDefinition("ClassIndexTestClass");
    compositeIndexEleven.addIndex(
        new OPropertyIndexDefinition("ClassIndexTestClass", "fFourteen", YTType.INTEGER));
    compositeIndexEleven.addIndex(
        new OPropertyRidBagIndexDefinition("ClassIndexTestClass", "fRidBag"));
    expectedIndexDefinitions.add(compositeIndexEleven);

    final OPropertyIndexDefinition propertyIndex =
        new OPropertyIndexDefinition("ClassIndexTestClass", "fOne", YTType.INTEGER);
    expectedIndexDefinitions.add(propertyIndex);

    final OPropertyIndexDefinition parentPropertyIndex =
        new OPropertyIndexDefinition("ClassIndexTestSuperClass", "fNine", YTType.INTEGER);
    expectedIndexDefinitions.add(parentPropertyIndex);

    final OPropertyMapIndexDefinition propertyMapIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyMapIndexDefinition);

    final OPropertyMapIndexDefinition propertyMapByValueIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fEmbeddedMap",
            YTType.INTEGER,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyMapByValueIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByKeyIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            YTType.STRING,
            OPropertyMapIndexDefinition.INDEX_BY.KEY);
    expectedIndexDefinitions.add(propertyLinkMapByKeyIndexDefinition);

    final OPropertyMapIndexDefinition propertyLinkMapByValueIndexDefinition =
        new OPropertyMapIndexDefinition(
            "ClassIndexTestClass",
            "fLinkMap",
            YTType.LINK,
            OPropertyMapIndexDefinition.INDEX_BY.VALUE);
    expectedIndexDefinitions.add(propertyLinkMapByValueIndexDefinition);

    assertEquals(indexes.size(), 18);

    for (final OIndex index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test
  public void testGetIndexesWithoutParent() {
    final YTClass inClass = database.getMetadata().getSchema().createClass("ClassIndexInTest");
    inClass.createProperty(database, "fOne", YTType.INTEGER);

    final OIndex result =
        inClass.createIndex(database,
            "ClassIndexInTestPropertyOne",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fOne"});

    assertEquals(result.getName(), "ClassIndexInTestPropertyOne");
    assertEquals(inClass.getClassIndex(database, "ClassIndexInTestPropertyOne").getName(),
        result.getName());

    final Set<OIndex> indexes = inClass.getIndexes(database);
    final OPropertyIndexDefinition propertyIndexDefinition =
        new OPropertyIndexDefinition("ClassIndexInTest", "fOne", YTType.INTEGER);

    assertEquals(indexes.size(), 1);

    assertEquals(propertyIndexDefinition, indexes.iterator().next().getDefinition());
  }

  @Test(expectedExceptions = YTIndexException.class)
  public void testCreateIndexEmptyFields() {
    oClass.createIndex(database, "ClassIndexTestCompositeEmpty", YTClass.INDEX_TYPE.UNIQUE);
  }

  @Test(expectedExceptions = YTIndexException.class)
  public void testCreateIndexAbsentFields() {
    oClass.createIndex(database,
        "ClassIndexTestCompositeFieldAbsent",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fFive"});
  }

  @Test
  public void testCreateProxyIndex() {
    try {
      oClass.createIndex(database, "ClassIndexTestProxyIndex", YTClass.INDEX_TYPE.PROXY, "fOne");
      Assert.fail();
    } catch (YTIndexException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "testGetInvolvedIndexesOnePropertyArrayParams")
  public void testCreateDictionaryIndex() {
    final OIndex result =
        oClass.createIndex(database, "ClassIndexTestDictionaryIndex", YTClass.INDEX_TYPE.DICTIONARY,
            "fOne");

    assertEquals(result.getName(), "ClassIndexTestDictionaryIndex");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestDictionaryIndex").getName(),
        result.getName());
    assertEquals(result.getType(), YTClass.INDEX_TYPE.DICTIONARY.toString());
  }

  @Test(dependsOnMethods = "testGetInvolvedIndexesOnePropertyArrayParams")
  public void testCreateNotUniqueIndex() {
    final OIndex result =
        oClass.createIndex(database, "ClassIndexTestNotUniqueIndex", YTClass.INDEX_TYPE.NOTUNIQUE,
            "fOne");

    assertEquals(result.getName(), "ClassIndexTestNotUniqueIndex");
    assertEquals(oClass.getClassIndex(database, "ClassIndexTestNotUniqueIndex").getName(),
        result.getName());
    assertEquals(result.getType(), YTClass.INDEX_TYPE.NOTUNIQUE.toString());
  }

  @Test
  public void testCreateMapWithoutLinkedType() {
    try {
      oClass.createIndex(database,
          "ClassIndexMapWithoutLinkedTypeIndex",
          YTClass.INDEX_TYPE.NOTUNIQUE, "fEmbeddedMapWithoutLinkedType by value");
      fail();
    } catch (YTIndexException e) {
      assertTrue(
          e.getMessage()
              .contains(
                  "Linked type was not provided. You should provide linked type for embedded"
                      + " collections that are going to be indexed."));
    }
  }

  public void createParentPropertyIndex() {
    final OIndex result =
        oSuperClass.createIndex(database,
            "ClassIndexTestParentPropertyNine",
            YTClass.INDEX_TYPE.UNIQUE.toString(),
            null,
            new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"fNine"});

    assertEquals(result.getName(), "ClassIndexTestParentPropertyNine");
    assertEquals(
        oSuperClass.getClassIndex(database, "ClassIndexTestParentPropertyNine").getName(),
        result.getName());
  }

  private boolean containsIndex(
      final Collection<? extends OIndex> classIndexes, final String indexName) {
    for (final OIndex index : classIndexes) {
      if (index.getName().equals(indexName)) {
        return true;
      }
    }
    return false;
  }

  @Test
  public void testDropProperty() throws Exception {
    oClass.createProperty(database, "fFive", YTType.INTEGER);

    oClass.dropProperty(database, "fFive");

    assertNull(oClass.getProperty("fFive"));
  }
}
