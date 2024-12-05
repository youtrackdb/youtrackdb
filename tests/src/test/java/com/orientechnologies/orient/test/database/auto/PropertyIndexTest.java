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

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
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

    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.createClass("PropertyIndexTestClass");
    oClass.createProperty(database, "prop0", YTType.LINK);
    oClass.createProperty(database, "prop1", YTType.STRING);
    oClass.createProperty(database, "prop2", YTType.INTEGER);
    oClass.createProperty(database, "prop3", YTType.BOOLEAN);
    oClass.createProperty(database, "prop4", YTType.INTEGER);
    oClass.createProperty(database, "prop5", YTType.STRING);
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
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");
    final YTProperty propOne = oClass.getProperty("prop1");

    propOne.createIndex(database, YTClass.INDEX_TYPE.UNIQUE,
        new YTEntityImpl().field("ignoreNullValues", true));

    final Collection<OIndex> indexes = propOne.getIndexes(database);
    OIndexDefinition indexDefinition = null;

    for (final OIndex index : indexes) {
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
    Assert.assertEquals(indexDefinition.getTypes()[0], YTType.STRING);
  }

  @Test(dependsOnMethods = {"testCreateUniqueIndex"})
  public void createAdditionalSchemas() {
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(database,
        "propOne0",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop0", "prop1"});
    oClass.createIndex(database,
        "propOne1",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop1", "prop2"});
    oClass.createIndex(database,
        "propOne2",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop1", "prop3"});
    oClass.createIndex(database,
        "propOne3",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop2", "prop3"});
    oClass.createIndex(database,
        "propOne4",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop2", "prop1"});
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetIndexes() {
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");
    final YTProperty propOne = oClass.getProperty("prop1");

    final Collection<OIndex> indexes = propOne.getIndexes(database);
    Assert.assertEquals(indexes.size(), 1);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetAllIndexes() {
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");
    final YTProperty propOne = oClass.getProperty("prop1");

    final Collection<OIndex> indexes = propOne.getAllIndexes(database);
    Assert.assertEquals(indexes.size(), 5);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne0"));
    Assert.assertNotNull(containsIndex(indexes, "propOne1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne2"));
    Assert.assertNotNull(containsIndex(indexes, "propOne4"));
  }

  @Test
  public void testIsIndexedNonIndexedField() {
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");
    final YTProperty propThree = oClass.getProperty("prop3");
    Assert.assertFalse(propThree.isIndexed(database));
  }

  @Test(dependsOnMethods = {"testCreateUniqueIndex"})
  public void testIsIndexedIndexedField() {
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");
    final YTProperty propOne = oClass.getProperty("prop1");
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
    YTEntityImpl doc =
        new YTEntityImpl("PropertyIndexTestClass").fields("prop1", "testComposite3");
    doc.save();
    new YTEntityImpl("PropertyIndexTestClass").fields("prop0", doc, "prop1", "testComposite1")
        .save();
    new YTEntityImpl("PropertyIndexTestClass").fields("prop0", doc).save();
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

    YTEntityImpl doc =
        new YTEntityImpl("PropertyIndexTestClass").fields("prop1", "testComposite34");
    doc.save();
    new YTEntityImpl("PropertyIndexTestClass").fields("prop0", doc, "prop1", "testComposite33")
        .save();
    new YTEntityImpl("PropertyIndexTestClass").fields("prop0", doc).save();

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
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(database,
        "PropertyIndexFirstIndex",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop4"});
    oClass.createIndex(database,
        "PropertyIndexSecondIndex",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop4"});

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
    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(database,
        "PropertyIndexFirstIndex",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop4"});
    oClass.createIndex(database,
        "PropertyIndexSecondIndex",
        YTClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        new YTEntityImpl().fields("ignoreNullValues", true), new String[]{"prop4", "prop5"});

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

  private OIndex containsIndex(final Collection<OIndex> indexes, final String indexName) {
    for (final OIndex index : indexes) {
      if (index.getName().equals(indexName)) {
        return index;
      }
    }
    return null;
  }
}
