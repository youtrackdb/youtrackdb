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

import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLDropClassIndexTest extends DocumentDBBaseTest {

  private static final YTType EXPECTED_PROP1_TYPE = YTType.DOUBLE;
  private static final YTType EXPECTED_PROP2_TYPE = YTType.INTEGER;

  @Parameters(value = "remote")
  public SQLDropClassIndexTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final YTSchema schema = database.getMetadata().getSchema();
    final YTClass oClass = schema.createClass("SQLDropClassTestClass");
    oClass.createProperty(database, "prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty(database, "prop2", EXPECTED_PROP2_TYPE);
  }

  @Test
  public void testIndexDeletion() throws Exception {
    database
        .command(
            "CREATE INDEX SQLDropClassCompositeIndex ON SQLDropClassTestClass (prop1, prop2)"
                + " UNIQUE")
        .close();

    Assert.assertNotNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SQLDropClassCompositeIndex"));

    database.command("DROP CLASS SQLDropClassTestClass").close();

    Assert.assertNull(database.getMetadata().getSchema().getClass("SQLDropClassTestClass"));
    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SQLDropClassCompositeIndex"));
    database.close();
    database = createSessionInstance();
    Assert.assertNull(database.getMetadata().getSchema().getClass("SQLDropClassTestClass"));
    Assert.assertNull(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "SQLDropClassCompositeIndex"));
  }
}
