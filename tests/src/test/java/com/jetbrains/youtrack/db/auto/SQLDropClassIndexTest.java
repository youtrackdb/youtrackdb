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
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.Schema;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = {"index"})
public class SQLDropClassIndexTest extends BaseDBTest {

  private static final PropertyType EXPECTED_PROP1_TYPE = PropertyType.DOUBLE;
  private static final PropertyType EXPECTED_PROP2_TYPE = PropertyType.INTEGER;

  @Parameters(value = "remote")
  public SQLDropClassIndexTest(boolean remote) {
    super(remote);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    final Schema schema = db.getMetadata().getSchema();
    final SchemaClass oClass = schema.createClass("SQLDropClassTestClass");
    oClass.createProperty(db, "prop1", EXPECTED_PROP1_TYPE);
    oClass.createProperty(db, "prop2", EXPECTED_PROP2_TYPE);
  }

  @Test
  public void testIndexDeletion() throws Exception {
    db
        .command(
            "CREATE INDEX SQLDropClassCompositeIndex ON SQLDropClassTestClass (prop1, prop2)"
                + " UNIQUE")
        .close();

    Assert.assertNotNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "SQLDropClassCompositeIndex"));

    db.command("DROP CLASS SQLDropClassTestClass").close();

    Assert.assertNull(db.getMetadata().getSchema().getClass("SQLDropClassTestClass"));
    Assert.assertNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "SQLDropClassCompositeIndex"));
    db.close();
    db = createSessionInstance();
    Assert.assertNull(db.getMetadata().getSchema().getClass("SQLDropClassTestClass"));
    Assert.assertNull(
        db
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "SQLDropClassCompositeIndex"));
  }
}
