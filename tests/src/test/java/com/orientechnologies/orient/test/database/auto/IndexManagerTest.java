package com.orientechnologies.orient.test.database.auto;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.common.listener.ProgressListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.CompositeIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.index.PropertyIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
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
public class IndexManagerTest extends DocumentDBBaseTest {

  private static final String CLASS_NAME = "classForIndexManagerTest";

  @Parameters(value = "remote")
  public IndexManagerTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();

    final SchemaClass oClass = schema.createClass(CLASS_NAME);

    oClass.createProperty(database, "fOne", PropertyType.INTEGER);
    oClass.createProperty(database, "fTwo", PropertyType.STRING);
    oClass.createProperty(database, "fThree", PropertyType.BOOLEAN);
    oClass.createProperty(database, "fFour", PropertyType.INTEGER);

    oClass.createProperty(database, "fSix", PropertyType.STRING);
    oClass.createProperty(database, "fSeven", PropertyType.STRING);
  }

  @Test
  public void testCreateOnePropertyIndexTest() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Index result =
        indexManager.createIndex(
            database,
            "propertyone",
            SchemaClass.INDEX_TYPE.UNIQUE.toString(),
            new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
            new int[]{database.getClusterIdByName(CLASS_NAME)},
            null,
            null);

    assertEquals(result.getName(), "propertyone");

    indexManager.reload(database);
    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, CLASS_NAME, "propertyone")
            .getName(),
        result.getName());
  }

  @Test
  public void createCompositeIndexTestWithoutListener() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Index result =
        indexManager.createIndex(
            database,
            "compositeone",
            SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
            new CompositeIndexDefinition(
                CLASS_NAME,
                Arrays.asList(
                    new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
                    new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING))),
            new int[]{database.getClusterIdByName(CLASS_NAME)},
            null,
            null);

    assertEquals(result.getName(), "compositeone");

    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, CLASS_NAME, "compositeone")
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

    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Index result =
        indexManager.createIndex(
            database,
            "compositetwo",
            SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
            new CompositeIndexDefinition(
                CLASS_NAME,
                Arrays.asList(
                    new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
                    new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING),
                    new PropertyIndexDefinition(CLASS_NAME, "fThree", PropertyType.BOOLEAN))),
            new int[]{database.getClusterIdByName(CLASS_NAME)},
            progressListener,
            null);

    assertEquals(result.getName(), "compositetwo");
    assertEquals(atomicInteger.get(), 2);

    assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getClassIndex(database, CLASS_NAME, "compositetwo")
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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, List.of("fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedDoesNotContainProperty() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, List.of("fSix"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedTwoProperties() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreeProperties() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesBrokenFiledNameCase() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesBrokenClassNameCase() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(
            "ClaSSForIndeXManagerTeST", Arrays.asList("fTwo", "fOne", "fThree"));

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesNotFirst() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fTree"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesMoreThanNeeded() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result =
        indexManager.areIndexed(CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedOnePropertyArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedDoesNotContainPropertyArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fSix");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedTwoPropertiesArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedThreePropertiesArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThree");

    assertTrue(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesNotFirstArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fTree");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testAreIndexedPropertiesMoreThanNeededArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final boolean result = indexManager.areIndexed(CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

    assertFalse(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOnePropertyArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result = indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fOne");

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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fTwo", "fOne");
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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fTwo", "fOne", "fThree");

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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, "fTwo", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeededArrayParams() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, "fTwo", "fOne", "fThee", "fFour");

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetInvolvedIndexesPropertiesMorThanNeeded() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesNotExistingClass() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, "testlass", List.of("fOne"));

    assertTrue(result.isEmpty());
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesOneProperty() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, List.of("fOne"));

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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, "ClaSSforindeXmanagerTEST", List.of("fOne"));

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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, Arrays.asList("fTwo", "fOne"));
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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThree"));

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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(database, CLASS_NAME, Arrays.asList("fTwo", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassInvolvedIndexesPropertiesMorThanNeeded() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> result =
        indexManager.getClassInvolvedIndexes(
            database, CLASS_NAME, Arrays.asList("fTwo", "fOne", "fThee", "fFour"));

    assertEquals(result.size(), 0);
  }

  @Test
  public void testGetClassInvolvedIndexesWithNullValues() {
    String className = "GetClassInvolvedIndexesWithNullValues";
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.createClass(className);

    oClass.createProperty(database, "one", PropertyType.STRING);
    oClass.createProperty(database, "two", PropertyType.STRING);
    oClass.createProperty(database, "three", PropertyType.STRING);

    indexManager.createIndex(
        database,
        className + "_indexOne_notunique",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new PropertyIndexDefinition(className, "one", PropertyType.STRING),
        oClass.getClusterIds(),
        null,
        null);

    indexManager.createIndex(
        database,
        className + "_indexOneTwo_notunique",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new CompositeIndexDefinition(
            className,
            Arrays.asList(
                new PropertyIndexDefinition(className, "one", PropertyType.STRING),
                new PropertyIndexDefinition(className, "two", PropertyType.STRING))),
        oClass.getClusterIds(),
        null,
        null);

    indexManager.createIndex(
        database,
        className + "_indexOneTwoThree_notunique",
        SchemaClass.INDEX_TYPE.NOTUNIQUE.toString(),
        new CompositeIndexDefinition(
            className,
            Arrays.asList(
                new PropertyIndexDefinition(className, "one", PropertyType.STRING),
                new PropertyIndexDefinition(className, "two", PropertyType.STRING),
                new PropertyIndexDefinition(className, "three", PropertyType.STRING))),
        oClass.getClusterIds(),
        null,
        null);

    Set<Index> result = indexManager.getClassInvolvedIndexes(database, className, List.of("one"));
    assertEquals(result.size(), 3);

    result = indexManager.getClassInvolvedIndexes(database, className, Arrays.asList("one", "two"));
    assertEquals(result.size(), 2);

    result =
        indexManager.getClassInvolvedIndexes(
            database, className, Arrays.asList("one", "two", "three"));
    assertEquals(result.size(), 1);

    result = indexManager.getClassInvolvedIndexes(database, className, List.of("two"));
    assertEquals(result.size(), 0);

    result =
        indexManager.getClassInvolvedIndexes(
            database, className, Arrays.asList("two", "one", "three"));
    assertEquals(result.size(), 1);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexes() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> indexes = indexManager.getClassIndexes(database, CLASS_NAME);
    final Set<IndexDefinition> expectedIndexDefinitions = new HashSet<IndexDefinition>();

    final CompositeIndexDefinition compositeIndexOne = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexOne.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexOne);

    final CompositeIndexDefinition compositeIndexTwo = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fThree", PropertyType.BOOLEAN));
    compositeIndexTwo.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexTwo);

    final PropertyIndexDefinition propertyIndex =
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final Index index : indexes) {
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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Set<Index> indexes = indexManager.getClassIndexes(database, "ClassforindeXMaNAgerTeST");
    final Set<IndexDefinition> expectedIndexDefinitions = new HashSet<IndexDefinition>();

    final CompositeIndexDefinition compositeIndexOne = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexOne.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexOne.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexOne);

    final CompositeIndexDefinition compositeIndexTwo = new CompositeIndexDefinition(CLASS_NAME);

    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fTwo", PropertyType.STRING));
    compositeIndexTwo.addIndex(
        new PropertyIndexDefinition(CLASS_NAME, "fThree", PropertyType.BOOLEAN));
    compositeIndexTwo.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(compositeIndexTwo);

    final PropertyIndexDefinition propertyIndex =
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER);
    propertyIndex.setNullValuesIgnored(false);
    expectedIndexDefinitions.add(propertyIndex);

    assertEquals(indexes.size(), 3);

    for (final Index index : indexes) {
      assertTrue(expectedIndexDefinitions.contains(index.getDefinition()));
    }
  }

  @Test
  public void testDropIndex() throws Exception {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    indexManager.createIndex(
        database,
        "anotherproperty",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        new PropertyIndexDefinition(CLASS_NAME, "fOne", PropertyType.INTEGER),
        new int[]{database.getClusterIdByName(CLASS_NAME)},
        null,
        null);

    assertNotNull(indexManager.getIndex(database, "anotherproperty"));
    assertNotNull(indexManager.getClassIndex(database, CLASS_NAME, "anotherproperty"));

    indexManager.dropIndex(database, "anotherproperty");

    assertNull(indexManager.getIndex(database, "anotherproperty"));
    assertNull(indexManager.getClassIndex(database, CLASS_NAME, "anotherproperty"));
  }

  @Test
  public void testDropAllClassIndexes() {
    final SchemaClass oClass =
        database.getMetadata().getSchema().createClass("indexManagerTestClassTwo");
    oClass.createProperty(database, "fOne", PropertyType.INTEGER);

    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    indexManager.createIndex(
        database,
        "twoclassproperty",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        new PropertyIndexDefinition("indexManagerTestClassTwo", "fOne", PropertyType.INTEGER),
        new int[]{database.getClusterIdByName("indexManagerTestClassTwo")},
        null,
        null);

    assertFalse(indexManager.getClassIndexes(database, "indexManagerTestClassTwo").isEmpty());

    indexManager.dropIndex(database, "twoclassproperty");

    assertTrue(indexManager.getClassIndexes(database, "indexManagerTestClassTwo").isEmpty());
  }

  @Test(dependsOnMethods = "testDropAllClassIndexes")
  public void testDropNonExistingClassIndex() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    indexManager.dropIndex(database, "twoclassproperty");
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndex() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Index result = indexManager.getClassIndex(database, CLASS_NAME, "propertyone");
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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Index result =
        indexManager.getClassIndex(database, "ClaSSforindeXManagerTeST", "propertyone");
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
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Index result = indexManager.getClassIndex(database, CLASS_NAME, "propertyonetwo");
    assertNull(result);
  }

  @Test(
      dependsOnMethods = {
          "createCompositeIndexTestWithListener",
          "createCompositeIndexTestWithoutListener",
          "testCreateOnePropertyIndexTest"
      })
  public void testGetClassIndexWrongClassName() {
    final IndexManagerAbstract indexManager = database.getMetadata().getIndexManagerInternal();

    final Index result = indexManager.getClassIndex(database, "testClassTT", "propertyone");
    assertNull(result);
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
}
