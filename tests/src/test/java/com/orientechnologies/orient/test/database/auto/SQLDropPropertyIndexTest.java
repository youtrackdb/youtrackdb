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

import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.index.OCompositeIndexDefinition;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import java.util.Arrays;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLDropPropertyIndexTest extends DocumentDBBaseTest {

  private static final YTType EXPECTED_PROP1_TYPE = YTType.DOUBLE;
  private static final YTType EXPECTED_PROP2_TYPE = YTType.INTEGER;

  @Parameters(value = "remote")
  public SQLDropPropertyIndexTest(boolean remote) {
    super(remote);
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();

    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.createClass("DropPropertyIndexTestClass");
    oClass.createProperty(database, "prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty(database, "prop2", EXPECTED_PROP2_TYPE);
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.command("drop class DropPropertyIndexTestClass").close();

    super.afterMethod();
  }

  @Test
  public void testForcePropertyEnabled() throws Exception {
    database
        .command(
            "CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop2,"
                + " prop1) UNIQUE")
        .close();

    OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("DropPropertyIndexTestClass")
            .getClassIndex(database, "DropPropertyIndexCompositeIndex");
    Assert.assertNotNull(index);

    database.command("DROP PROPERTY DropPropertyIndexTestClass.prop1 FORCE").close();

    index =
        database
            .getMetadata()
            .getSchema()
            .getClass("DropPropertyIndexTestClass")
            .getClassIndex(database, "DropPropertyIndexCompositeIndex");

    Assert.assertNull(index);
  }

  @Test
  public void testForcePropertyEnabledBrokenCase() throws Exception {
    database
        .command(
            "CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop2,"
                + " prop1) UNIQUE")
        .close();

    OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("DropPropertyIndexTestClass")
            .getClassIndex(database, "DropPropertyIndexCompositeIndex");
    Assert.assertNotNull(index);

    database.command("DROP PROPERTY DropPropertyIndextestclasS.prop1 FORCE").close();

    index =
        database
            .getMetadata()
            .getSchema()
            .getClass("DropPropertyIndexTestClass")
            .getClassIndex(database, "DropPropertyIndexCompositeIndex");

    Assert.assertNull(index);
  }

  @Test
  public void testForcePropertyDisabled() throws Exception {
    database
        .command(
            "CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop1,"
                + " prop2) UNIQUE")
        .close();

    OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("DropPropertyIndexTestClass")
            .getClassIndex(database, "DropPropertyIndexCompositeIndex");
    Assert.assertNotNull(index);

    try {
      database.command("DROP PROPERTY DropPropertyIndexTestClass.prop1").close();
      Assert.fail();
    } catch (YTCommandExecutionException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Property used in indexes (DropPropertyIndexCompositeIndex). Please drop these"
                      + " indexes before removing property or use FORCE parameter."));
    }

    index =
        database
            .getMetadata()
            .getSchema()
            .getClass("DropPropertyIndexTestClass")
            .getClassIndex(database, "DropPropertyIndexCompositeIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new YTType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");
  }

  @Test
  public void testForcePropertyDisabledBrokenCase() throws Exception {
    database
        .command(
            "CREATE INDEX DropPropertyIndexCompositeIndex ON DropPropertyIndexTestClass (prop1,"
                + " prop2) UNIQUE")
        .close();

    try {
      database.command("DROP PROPERTY DropPropertyIndextestclass.prop1").close();
      Assert.fail();
    } catch (YTCommandExecutionException e) {
      Assert.assertTrue(
          e.getMessage()
              .contains(
                  "Property used in indexes (DropPropertyIndexCompositeIndex). Please drop these"
                      + " indexes before removing property or use FORCE parameter."));
    }

    final OIndex index =
        database
            .getMetadata()
            .getSchema()
            .getClass("DropPropertyIndexTestClass")
            .getClassIndex(database, "DropPropertyIndexCompositeIndex");

    Assert.assertNotNull(index);

    final OIndexDefinition indexDefinition = index.getDefinition();

    Assert.assertTrue(indexDefinition instanceof OCompositeIndexDefinition);
    Assert.assertEquals(indexDefinition.getFields(), Arrays.asList("prop1", "prop2"));
    Assert.assertEquals(
        indexDefinition.getTypes(), new YTType[]{EXPECTED_PROP1_TYPE, EXPECTED_PROP2_TYPE});
    Assert.assertEquals(index.getType(), "UNIQUE");
  }
}
