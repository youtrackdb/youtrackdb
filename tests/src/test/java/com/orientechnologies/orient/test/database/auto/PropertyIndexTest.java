/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Property;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class PropertyIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public PropertyIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.createClass("PropertyIndexTestClass");
    oClass.createProperty(database, "prop0", PropertyType.LINK);
    oClass.createProperty(database, "prop1", PropertyType.STRING);
    oClass.createProperty(database, "prop2", PropertyType.INTEGER);
    oClass.createProperty(database, "prop3", PropertyType.BOOLEAN);
    oClass.createProperty(database, "prop4", PropertyType.INTEGER);
    oClass.createProperty(database, "prop5", PropertyType.STRING);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (database.isClosed()) {
      database = createSessionInstance();
    }

    database.begin();
    database.command("delete from PropertyIndexTestClass");
    database.commit();

    database.command("drop class PropertyIndexTestClass");

    super.afterClass();
  }

  @Test
  public void testCreateUniqueIndex() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");
    final Property propOne = oClass.getProperty("prop1");

    propOne.createIndex(database, SchemaClass.INDEX_TYPE.UNIQUE,
        new EntityImpl().field("ignoreNullValues", true));

    final Collection<Index> indexes = propOne.getIndexes(database);
    IndexDefinition indexDefinition = null;

    for (final Index index : indexes) {
      if (index.getName().equals("PropertyIndexTestClass.prop1")) {
        indexDefinition = index.getDefinition();
        break;
      }
    }

    Assert.assertNotNull(indexDefinition);
    Assert.assertEquals(indexDefinition.getParamCount(), 1);
    Assert.assertEquals(indexDefinition.getFields().size(), 1);
    Assert.assertTrue(indexDefinition.getFields().contains("prop1"));
    Assert.assertEquals(indexDefinition.getTypes().length, 1);
    Assert.assertEquals(indexDefinition.getTypes()[0], PropertyType.STRING);
  }

  @Test(dependsOnMethods = {"testCreateUniqueIndex"})
  public void createAdditionalSchemas() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(database,
        "propOne0",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop0", "prop1"});
    oClass.createIndex(database,
        "propOne1",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop1", "prop2"});
    oClass.createIndex(database,
        "propOne2",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop1", "prop3"});
    oClass.createIndex(database,
        "propOne3",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop2", "prop3"});
    oClass.createIndex(database,
        "propOne4",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop2", "prop1"});
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetIndexes() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");
    final Property propOne = oClass.getProperty("prop1");

    final Collection<Index> indexes = propOne.getIndexes(database);
    Assert.assertEquals(indexes.size(), 1);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetAllIndexes() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");
    final Property propOne = oClass.getProperty("prop1");

    final Collection<Index> indexes = propOne.getAllIndexes(database);
    Assert.assertEquals(indexes.size(), 5);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne0"));
    Assert.assertNotNull(containsIndex(indexes, "propOne1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne2"));
    Assert.assertNotNull(containsIndex(indexes, "propOne4"));
  }

  @Test
  public void testIsIndexedNonIndexedField() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");
    final Property propThree = oClass.getProperty("prop3");
    Assert.assertFalse(propThree.isIndexed(database));
  }

  @Test(dependsOnMethods = {"testCreateUniqueIndex"})
  public void testIsIndexedIndexedField() {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");
    final Property propOne = oClass.getProperty("prop1");
    Assert.assertTrue(propOne.isIndexed(database));
  }

  @Test(dependsOnMethods = {"testIsIndexedIndexedField"})
  public void testIndexingCompositeRIDAndOthers() throws Exception {
    checkEmbeddedDB();

    long prev0 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size(database);
    long prev1 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size(database);

    database.begin();
    EntityImpl doc =
        new EntityImpl("PropertyIndexTestClass").fields("prop1", "testComposite3");
    doc.save();
    new EntityImpl("PropertyIndexTestClass").fields("prop0", doc, "prop1", "testComposite1")
        .save();
    new EntityImpl("PropertyIndexTestClass").fields("prop0", doc).save();
    database.commit();

    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size(database),
        prev0 + 1);
    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size(database),
        prev1);
  }

  @Test(dependsOnMethods = {"testIndexingCompositeRIDAndOthers"})
  public void testIndexingCompositeRIDAndOthersInTx() throws Exception {
    database.begin();

    long prev0 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size(database);
    long prev1 =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size(database);

    EntityImpl doc =
        new EntityImpl("PropertyIndexTestClass").fields("prop1", "testComposite34");
    doc.save();
    new EntityImpl("PropertyIndexTestClass").fields("prop0", doc, "prop1", "testComposite33")
        .save();
    new EntityImpl("PropertyIndexTestClass").fields("prop0", doc).save();

    database.commit();

    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne0")
            .getInternal()
            .size(database),
        prev0 + 1);
    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "propOne1")
            .getInternal()
            .size(database),
        prev1);
  }

  @Test
  public void testDropIndexes() throws Exception {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(database,
        "PropertyIndexFirstIndex",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop4"});
    oClass.createIndex(database,
        "PropertyIndexSecondIndex",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop4"});

    oClass.getProperty("prop4").dropIndexes(database);

    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexFirstIndex"));
    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexSecondIndex"));
  }

  @Test
  public void testDropIndexesForComposite() throws Exception {
    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(database,
        "PropertyIndexFirstIndex",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop4"});
    oClass.createIndex(database,
        "PropertyIndexSecondIndex",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new EntityImpl().fields("ignoreNullValues", true), new String[]{"prop4", "prop5"});

    try {
      oClass.getProperty("prop4").dropIndexes(database);
      Assert.fail();
    } catch (IllegalArgumentException e) {
      Assert.assertTrue(
          e.getMessage().contains("This operation applicable only for property indexes. "));
    }

    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexFirstIndex"));
    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "PropertyIndexSecondIndex"));
  }

  private Index containsIndex(final Collection<Index> indexes, final String indexName) {
    for (final Index index : indexes) {
      if (index.getName().equals(indexName)) {
        return index;
      }
    }
    return null;
  }
}
