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
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SchemaPropertyIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SchemaPropertyIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = db.getMetadata().getSchema();
    final var oClass = schema.createClass("PropertyIndexTestClass");
    oClass.createProperty(db, "prop0", PropertyType.LINK);
    oClass.createProperty(db, "prop1", PropertyType.STRING);
    oClass.createProperty(db, "prop2", PropertyType.INTEGER);
    oClass.createProperty(db, "prop3", PropertyType.BOOLEAN);
    oClass.createProperty(db, "prop4", PropertyType.INTEGER);
    oClass.createProperty(db, "prop5", PropertyType.STRING);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (db.isClosed()) {
      db = createSessionInstance();
    }

    db.begin();
    db.command("delete from PropertyIndexTestClass");
    db.commit();

    db.command("drop class PropertyIndexTestClass");

    super.afterClass();
  }

  @Test
  public void testCreateUniqueIndex() {
    var schema = db.getMetadata().getSchema();
    var oClass = schema.getClassInternal("PropertyIndexTestClass");
    final var propOne = oClass.getProperty("prop1");

    propOne.createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE,
        Map.of("ignoreNullValues", true));

    final Collection<Index> indexes = oClass.getInvolvedIndexesInternal(db, "prop1");
    IndexDefinition indexDefinition = null;

    for (final var index : indexes) {
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
    final Schema schema = db.getMetadata().getSchema();
    final var oClass = schema.getClass("PropertyIndexTestClass");

    oClass.createIndex(db,
        "propOne0",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop0", "prop1"});
    oClass.createIndex(db,
        "propOne1",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop1", "prop2"});
    oClass.createIndex(db,
        "propOne2",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop1", "prop3"});
    oClass.createIndex(db,
        "propOne3",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop2", "prop3"});
    oClass.createIndex(db,
        "propOne4",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop2", "prop1"});
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetIndexes() {
    var schema = db.getMetadata().getSchema();
    var oClass = schema.getClassInternal("PropertyIndexTestClass");
    oClass.getProperty("prop1");

    var indexes = oClass.getInvolvedIndexesInternal(db, "prop1");
    Assert.assertEquals(indexes.size(), 1);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
  }

  @Test(dependsOnMethods = "createAdditionalSchemas")
  public void testGetAllIndexes() {
    var schema = db.getMetadata().getSchema();
    var oClass = schema.getClassInternal("PropertyIndexTestClass");
    var propOne = oClass.getPropertyInternal("prop1");

    final var indexes = propOne.getAllIndexesInternal(db);
    Assert.assertEquals(indexes.size(), 5);
    Assert.assertNotNull(containsIndex(indexes, "PropertyIndexTestClass.prop1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne0"));
    Assert.assertNotNull(containsIndex(indexes, "propOne1"));
    Assert.assertNotNull(containsIndex(indexes, "propOne2"));
    Assert.assertNotNull(containsIndex(indexes, "propOne4"));
  }

  @Test
  public void testIsIndexedNonIndexedField() {
    var schema = db.getMetadata().getSchema();
    var oClass = schema.getClassInternal("PropertyIndexTestClass");
    var propThree = oClass.getPropertyInternal("prop3");

    Assert.assertTrue(propThree.getAllIndexes(db).isEmpty());
  }

  @Test(dependsOnMethods = {"testCreateUniqueIndex"})
  public void testIsIndexedIndexedField() {
    var schema = db.getMetadata().getSchema();
    var oClass = schema.getClassInternal("PropertyIndexTestClass");
    var propOne = oClass.getPropertyInternal("prop1");
    Assert.assertFalse(propOne.getAllIndexes(db).isEmpty());
  }

  @Test(dependsOnMethods = {"testIsIndexedIndexedField"})
  public void testIndexingCompositeRIDAndOthers() throws Exception {
    checkEmbeddedDB();

    var prev0 =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne0")
            .getInternal()
            .size(db);
    var prev1 =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne1")
            .getInternal()
            .size(db);

    db.begin();
    var doc =
        ((EntityImpl) db.newEntity("PropertyIndexTestClass")).fields("prop1", "testComposite3");
    doc.save();
    ((EntityImpl) db.newEntity("PropertyIndexTestClass")).fields("prop0", doc, "prop1",
            "testComposite1")
        .save();
    ((EntityImpl) db.newEntity("PropertyIndexTestClass")).fields("prop0", doc).save();
    db.commit();

    Assert.assertEquals(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne0")
            .getInternal()
            .size(db),
        prev0 + 1);
    Assert.assertEquals(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne1")
            .getInternal()
            .size(db),
        prev1);
  }

  @Test(dependsOnMethods = {"testIndexingCompositeRIDAndOthers"})
  public void testIndexingCompositeRIDAndOthersInTx() throws Exception {
    db.begin();

    var prev0 =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne0")
            .getInternal()
            .size(db);
    var prev1 =
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne1")
            .getInternal()
            .size(db);

    var doc =
        ((EntityImpl) db.newEntity("PropertyIndexTestClass")).fields("prop1", "testComposite34");
    doc.save();
    ((EntityImpl) db.newEntity("PropertyIndexTestClass")).fields("prop0", doc, "prop1",
            "testComposite33")
        .save();
    ((EntityImpl) db.newEntity("PropertyIndexTestClass")).fields("prop0", doc).save();

    db.commit();

    Assert.assertEquals(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne0")
            .getInternal()
            .size(db),
        prev0 + 1);
    Assert.assertEquals(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "propOne1")
            .getInternal()
            .size(db),
        prev1);
  }

  @Test
  public void testDropIndexes() throws Exception {
    var schema = db.getMetadata().getSchema();
    var oClass = schema.getClassInternal("PropertyIndexTestClass");

    oClass.createIndex(db,
        "PropertyIndexFirstIndex",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop4"});

    oClass.createIndex(db,
        "PropertyIndexSecondIndex",
        SchemaClass.INDEX_TYPE.UNIQUE.toString(),
        null,
        Map.of("ignoreNullValues", true), new String[]{"prop4"});

    var indexes = oClass.getInvolvedIndexes(db, "prop4");
    for (var index : indexes) {
      db.getMetadata().getIndexManagerInternal().dropIndex(db, index);
    }

    Assert.assertNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "PropertyIndexFirstIndex"));
    Assert.assertNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "PropertyIndexSecondIndex"));
  }

  private static Index containsIndex(final Collection<Index> indexes, final String indexName) {
    for (final var index : indexes) {
      if (index.getName().equals(indexName)) {
        return index;
      }
    }
    return null;
  }
}
