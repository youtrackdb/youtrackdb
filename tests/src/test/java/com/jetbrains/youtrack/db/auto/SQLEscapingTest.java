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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-select")
public class SQLEscapingTest extends BaseDBTest {

  private DatabaseSessionInternal database;

  @Parameters(value = "remote")
  public SQLEscapingTest(boolean remote) {
    super(remote);
  }

  // TODO re-enable this with new parser. this test was broken!!!
  //  public void testEscaping() {
  //    database.getMetadata().getSchema().createClass("Thing");
  //
  //    List result = database.command(new CommandSQL("select from cluster:internal")).execute();
  //
  //    List result0 = database.command(new CommandSQL("select from cluster:internal where
  // \"\\u005C\\u005C\" = \"\\u005C\\u005C\"")).execute();
  //    Assert.assertEquals(result.size(), result0.size());
  //
  //    EntityImpl document0 = database.command(new CommandSQL("insert into Thing set value =
  // \"\\u005C\\u005C\"")).execute();
  //    Assert.assertEquals("\\", document0.field("value"));
  //
  //    EntityImpl document1 = database.command(new CommandSQL("insert into Thing set value =
  // \"\\\\\"")).execute();
  //    Assert.assertEquals("\\", document1.field("value"));
  //
  //    List list1 = database.command(new CommandSQL("select from cluster:internal where
  // \"\\u005C\\u005C\" == \"\\\\\"")).execute();
  //    Assert.assertEquals(result.size(), list1.size());
  //
  //    try {
  //      EntityImpl document2 = database.command(new CommandSQL("insert into Thing set value =
  // \"\\\"")).execute();
  //      Assert.assertTrue(false);
  //    } catch (Exception e) {
  //      Assert.assertTrue(true);
  //    }
  //
  //    try {
  //      List list2 = database.command(new CommandSQL("select from cluster:internal where
  // \"\\u005C\" == \"\\\"")).execute();
  //      Assert.assertTrue(false);
  //    } catch (Exception e) {
  //      Assert.assertTrue(true);
  //    }
  //
  //    try {
  //      List list3 = database.command(new CommandSQL("select from cluster:internal where \"\\\"
  // == \"\\u005C\"")).execute();
  //      Assert.assertTrue(false);
  //    } catch (Exception e) {
  //      Assert.assertTrue(true);
  //    }
  //  }
}
