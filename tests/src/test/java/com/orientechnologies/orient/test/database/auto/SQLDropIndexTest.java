/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLDropIndexTest extends DocumentDBBaseTest {

  private static final OType EXPECTED_PROP1_TYPE = OType.DOUBLE;
  private static final OType EXPECTED_PROP2_TYPE = OType.INTEGER;

  @Parameters(value = "remote")
  public SQLDropIndexTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final OSchema schema = database.getMetadata().getSchema();
    final OClass oClass = schema.createClass("SQLDropIndexTestClass");
    oClass.createProperty("prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty("prop2", EXPECTED_PROP2_TYPE);
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

    database.getMetadata().getIndexManagerInternal().reload();

    OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("SQLDropIndexTestClass")
            .getClassIndex("SQLDropIndexTestClass.prop1");
    Assert.assertNotNull(index);

    database.command("DROP INDEX SQLDropIndexTestClass.prop1").close();
    database.getMetadata().getIndexManagerInternal().reload();

    index =
        database
            .getMetadata()
            .getSchema()
            .getClass("SQLDropIndexTestClass")
            .getClassIndex("SQLDropIndexTestClass.prop1");
    Assert.assertNull(index);
  }

  @Test(dependsOnMethods = "testOldSyntax")
  public void testDropCompositeIndex() throws Exception {
    database
        .command(
            "CREATE INDEX SQLDropIndexCompositeIndex ON SQLDropIndexTestClass (prop1, prop2)"
                + " UNIQUE")
        .close();
    database.getMetadata().getIndexManagerInternal().reload();

    OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("SQLDropIndexTestClass")
            .getClassIndex("SQLDropIndexCompositeIndex");
    Assert.assertNotNull(index);

    database.command("DROP INDEX SQLDropIndexCompositeIndex").close();
    database.getMetadata().getIndexManagerInternal().reload();

    index =
        database
            .getMetadata()
            .getSchema()
            .getClass("SQLDropIndexTestClass")
            .getClassIndex("SQLDropIndexCompositeIndex");
    Assert.assertNull(index);
  }

  @Test(dependsOnMethods = "testDropCompositeIndex")
  public void testDropIndexWorkedCorrectly() {
    OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("SQLDropIndexTestClass")
            .getClassIndex("SQLDropIndexTestClass.prop1");
    Assert.assertNull(index);
    index =
        database
            .getMetadata()
            .getSchema()
            .getClass("SQLDropIndexTestClass")
            .getClassIndex("SQLDropIndexWithoutClass");
    Assert.assertNull(index);
    index =
        database
            .getMetadata()
            .getSchema()
            .getClass("SQLDropIndexTestClass")
            .getClassIndex("SQLDropIndexCompositeIndex");
    Assert.assertNull(index);
  }
}
