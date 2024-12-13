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
import com.jetbrains.youtrack.db.internal.core.index.Index;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLDropIndexTest extends BaseDBTest {

  private static final PropertyType EXPECTED_PROP1_TYPE = PropertyType.DOUBLE;
  private static final PropertyType EXPECTED_PROP2_TYPE = PropertyType.INTEGER;

  @Parameters(value = "remote")
  public SQLDropIndexTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = database.getMetadata().getSchema();
    final SchemaClass oClass = schema.createClass("SQLDropIndexTestClass");
    oClass.createProperty(database, "prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty(database, "prop2", EXPECTED_PROP2_TYPE);
  }

  @AfterClass
  public void afterClass() throws Exception {
    if (database.isClosed()) {
      database = createSessionInstance();
    }

    database.command("delete from SQLDropIndexTestClass").close();
    database.command("drop class SQLDropIndexTestClass").close();

    super.afterClass();
  }

  @Test
  public void testOldSyntax() throws Exception {
    database.command("CREATE INDEX SQLDropIndexTestClass.prop1 UNIQUE").close();

    Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("SQLDropIndexTestClass")
            .getClassIndex(database, "SQLDropIndexTestClass.prop1");
    Assert.assertNotNull(index);

    database.command("DROP INDEX SQLDropIndexTestClass.prop1").close();

    index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("SQLDropIndexTestClass")
            .getClassIndex(database, "SQLDropIndexTestClass.prop1");
    Assert.assertNull(index);
  }

  @Test(dependsOnMethods = "testOldSyntax")
  public void testDropCompositeIndex() throws Exception {
    database
        .command(
            "CREATE INDEX SQLDropIndexCompositeIndex ON SQLDropIndexTestClass (prop1, prop2)"
                + " UNIQUE")
        .close();

    Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("SQLDropIndexTestClass")
            .getClassIndex(database, "SQLDropIndexCompositeIndex");
    Assert.assertNotNull(index);

    database.command("DROP INDEX SQLDropIndexCompositeIndex").close();

    index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("SQLDropIndexTestClass")
            .getClassIndex(database, "SQLDropIndexCompositeIndex");
    Assert.assertNull(index);
  }

  @Test(dependsOnMethods = "testDropCompositeIndex")
  public void testDropIndexWorkedCorrectly() {
    Index index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("SQLDropIndexTestClass")
            .getClassIndex(database, "SQLDropIndexTestClass.prop1");
    Assert.assertNull(index);
    index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("SQLDropIndexTestClass")
            .getClassIndex(database, "SQLDropIndexWithoutClass");
    Assert.assertNull(index);
    index =
        database
            .getMetadata()
            .getSchema()
            .getClassInternal("SQLDropIndexTestClass")
            .getClassIndex(database, "SQLDropIndexCompositeIndex");
    Assert.assertNull(index);
  }
}
