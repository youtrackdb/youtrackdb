package com.jetbrains.youtrack.db.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.CompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.PropertyIndexDefinition;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexManagerTest extends BaseDBTest {

  private static final String CLASS_NAME = "classForIndexManagerTest";

  @Parameters(value = "remote")
  public IndexManagerTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = session.getMetadata().getSchema();

    final var oClass = schema.createClass(CLASS_NAME);

    oClass.createProperty(session, "fOne", PropertyType.INTEGER);
    oClass.createProperty(session, "fTwo", PropertyType.STRING);
    oClass.createProperty(session, "fThree", PropertyType.BOOLEAN);
    oClass.createProperty(session, "fFour", PropertyType.INTEGER);

    oClass.createProperty(session, "fSix", PropertyType.STRING);
    oClass.createProperty(session, "fSeven", PropertyType.STRING);
  }

  @Test
  public void testCreateOnePropertyIndexTest() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.createIndex(
            session,
            "propertyone",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
            new int[]{session.getClusterIdByName(CLASS_NAME)},
            null,
            null);

    assertEquals(result.getName(), "propertyone");

    indexManager.reload(session);
    assertEquals(
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(session, CLASS_NAME, "propertyone")
            .getName(),
        result.getName());
  }

  @Test
  public void createCompositeIndexTestWithoutListener() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.createIndex(
            session,
            "compositeone",
            SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
            new CompositeIndexDefinition(
                CLASS_NAME,
                Arrays.asList(
                    new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
                    new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING))),
            new int[]{session.getClusterIdByName(CLASS_NAME)},
            null,
            null);

    assertEquals(result.getName(), "compositeone");

    assertEquals(
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(session, CLASS_NAME, "compositeone")
            .getName(),
        result.getName());
  }

  @Test
  public void createCompositeIndexTestWithListener() {
    final var atomicInteger = new AtomicInteger(0);
    final var progressListener =
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

    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.createIndex(
            session,
            "compositetwo",
            SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
            new CompositeIndexDefinition(
                CLASS_NAME,
                Arrays.asList(
                    new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
                    new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING),
                    new PropertyIndexDefinition(CLASS_NAME, "fThree", PropertyType.BOOLEAN))),
            new int[]{session.getClusterIdByName(CLASS_NAME)},
            progressListener,
            null);

    assertEquals(result.getName(), "compositetwo");
    assertEquals(atomicInteger.get(), 2);

    assertEquals(
        session
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(session, CLASS_NAME, "compositetwo")
            .getName(),
        result.getName());
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedOneProperty() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, List.of("fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedDoesNotContainProperty() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, List.of("fSix"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedTwoProperties() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, Arrays.asList("fTwo", "fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreeProperties() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.areIndexed(session, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesBrokenFiledNameCase() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.areIndexed(session, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesBrokenClassNameCase() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.areIndexed(
            session, "ClaSSForIndeXManagerTeST", Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesNotFirst() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, Arrays.asList("fTwo", "fTree"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesMoreThanNeeded() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.areIndexed(session, CLASS_NAME,
            Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedOnePropertyArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedDoesNotContainPropertyArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, "fSix");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedTwoPropertiesArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, "fTwo", "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, "fTwo", "fOne", "fThree");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesNotFirstArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, "fTwo", "fTree");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.areIndexed(session, CLASS_NAME, "fTwo", "fOne", "fThee",
        "fFour");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.getClassInvolvedIndexes(session, CLASS_NAME, "fOne");

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesTwoPropertiesArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, CLASS_NAME, "fTwo", "fOne");
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesThreePropertiesArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, CLASS_NAME, "fTwo", "fOne", "fThree");

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesNotInvolvedPropertiesArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, CLASS_NAME, "fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(
            session, CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(
            session, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesNotExistingClass() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, "testlass", List.of("fOne"));

    assertTrue(result.isEmpty());
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOneProperty() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, CLASS_NAME, List.of("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOnePropertyBrokenClassNameCase() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, "ClaSSforindeXmanagerTEST", List.of("fOne"));

    assertEquals(result.size(), 3);

    assertTrue(containsIndex(result, "propertyone"));
    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesTwoProperties() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, CLASS_NAME, Arrays.asList("fTwo", "fOne"));
    assertEquals(result.size(), 2);

    assertTrue(containsIndex(result, "compositeone"));
    assertTrue(containsIndex(result, "compositetwo"));
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesThreeProperties() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(
            session, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesThreePropertiesBrokenFiledNameTest() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(
            session, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertEquals(result.size(), 1);
    assertEquals(result.iterator().next().getName(), "compositetwo");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesNotInvolvedProperties() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(session, CLASS_NAME, Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassInvolvedIndexes(
            session, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetClassInvolvedIndexesWithNullValues() {
    var className = "GetClassInvolvedIndexesWithNullValues";
    final var indexManager = session.getMetadata().getIndexManagerInternal();
    final Schema schema = session.getMetadata().getSchema();
    final var oClass = schema.createClass(className);

    oClass.createProperty(session, "one", PropertyType.STRING);
    oClass.createProperty(session, "two", PropertyType.STRING);
    oClass.createProperty(session, "three", PropertyType.STRING);

    indexManager.createIndex(
        session,
        className + "_indexOne_notunique",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new PropertyIndexDefinition(className, "one", PropertyType.STRING),
        oClass.getClusterIds(session),
        null,
        null);

    indexManager.createIndex(
        session,
        className + "_indexOneTwo_notunique",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new CompositeIndexDefinition(
            className,
            Arrays.asList(
                new PropertyIndexDefinition(className, "one", PropertyType.STRING),
                new PropertyIndexDefinition(className, "two", PropertyType.STRING))),
        oClass.getClusterIds(session),
        null,
        null);

    indexManager.createIndex(
        session,
        className + "_indexOneTwoThree_notunique",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new CompositeIndexDefinition(
            className,
            Arrays.asList(
                new PropertyIndexDefinition(className, "one", PropertyType.STRING),
                new PropertyIndexDefinition(className, "two", PropertyType.STRING),
                new PropertyIndexDefinition(className, "three", PropertyType.STRING))),
        oClass.getClusterIds(session),
        null,
        null);

    var result = indexManager.getClassInvolvedIndexes(session, className, List.of("one"));
    assertEquals(result.size(), 3);

    result = indexManager.getClassInvolvedIndexes(session, className, Arrays.asList("one", "two"));
    assertEquals(result.size(), 2);

    result =
        indexManager.getClassInvolvedIndexes(
            session, className, Arrays.asList("one", "two", "three"));
    assertEquals(result.size(), 1);

    result = indexManager.getClassInvolvedIndexes(session, className, List.of("two"));
    assertEquals(result.size(), 0);

    result =
        indexManager.getClassInvolvedIndexes(
            session, className, Arrays.asList("two", "one", "three"));
    assertEquals(result.size(), 1);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexes() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var indexes = indexManager.getClassIndexes(session, CLASS_NAME);
    final Set<IndexDefinition> expectedIndexDefinitions = new HashSet<IndexDefinition>();

    final var compositeIndexOne = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexOne.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexOne);

    final var compositeIndexTwo = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fThree", PropertyType.BOOLEAN));
    compositeIndexTwo.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexTwo);

    final var propertyIndex =
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final var index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexesBrokenClassNameCase() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var indexes = indexManager.getClassIndexes(session, "ClassforindeXMaNAgerTeST");
    final Set<IndexDefinition> expectedIndexDefinitions = new HashSet<IndexDefinition>();

    final var compositeIndexOne = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexOne.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexOne);

    final var compositeIndexTwo = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fThree", PropertyType.BOOLEAN));
    compositeIndexTwo.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexTwo);

    final var propertyIndex =
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final var index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test
  public void testDropIndex() throws Exception {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    indexManager.createIndex(
        session,
        "anotherproperty",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
        new int[]{session.getClusterIdByName(CLASS_NAME)},
        null,
        null);

    assertNotNull(indexManager.getIndex(session, "anotherproperty"));
    assertNotNull(indexManager.getClassIndex(session, CLASS_NAME, "anotherproperty"));

    indexManager.dropIndex(session, "anotherproperty");

    assertNull(indexManager.getIndex(session, "anotherproperty"));
    assertNull(indexManager.getClassIndex(session, CLASS_NAME, "anotherproperty"));
  }

  @Test
  public void testDropAllClassIndexes() {
    final var oClass =
        session.getMetadata().getSchema().createClass("indexManagerTestClassTwo");
    oClass.createProperty(session, "fOne", PropertyType.INTEGER);

    final var indexManager = session.getMetadata().getIndexManagerInternal();

    indexManager.createIndex(
        session,
        "twoclassproperty",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        new PropertyIndexDefinition("indexManagerTestClassTwo", "fOne", PropertyType.INTEGER),
        new int[]{session.getClusterIdByName("indexManagerTestClassTwo")},
        null,
        null);

    assertFalse(indexManager.getClassIndexes(session, "indexManagerTestClassTwo").isEmpty());

    indexManager.dropIndex(session, "twoclassproperty");

    assertTrue(indexManager.getClassIndexes(session, "indexManagerTestClassTwo").isEmpty());
  }

  @Test(dependsOnMethods = "testDropAllClassIndexes")
  public void testDropNonExistingClassIndex() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    indexManager.dropIndex(session, "twoclassproperty");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndex() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.getClassIndex(session, CLASS_NAME, "propertyone");
    assertNotNull(result);
    assertEquals(result.getName(), "propertyone");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexBrokenClassNameCase() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result =
        indexManager.getClassIndex(session, "ClaSSforindeXManagerTeST", "propertyone");
    assertNotNull(result);
    assertEquals(result.getName(), "propertyone");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexWrongIndexName() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.getClassIndex(session, CLASS_NAME, "propertyonetwo");
    assertNull(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexWrongClassName() {
    final var indexManager = session.getMetadata().getIndexManagerInternal();

    final var result = indexManager.getClassIndex(session, "testClassTT", "propertyone");
    assertNull(result);
  }

  private boolean containsIndex(
      final Collection<? extends Index> classIndexes, final String indexName) {
    for (final var index : classIndexes) {
      if (index.getName().equals(indexName)) {
        return true;
      }
    }
    return false;
  }
}
