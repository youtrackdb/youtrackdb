/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.metadata.schema.YTSchema;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCommandExecutorSQLDeleteVertexTest extends DBTestBase {

  public void beforeTest() throws Exception {
    super.beforeTest();
    final YTSchema schema = db.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
  }

  @Test
  public void testDeleteVertexLimit() throws Exception {
    // for issue #4148

    for (int i = 0; i < 10; i++) {
      db.begin();
      db.command("create vertex User set name = 'foo" + i + "'").close();
      db.commit();
    }

    db.begin();
    db.command("delete vertex User limit 4").close();
    db.commit();

    YTResultSet result = db.query("select from User");
    Assert.assertEquals(result.stream().count(), 6);
  }

  @Test
  public void testDeleteVertexBatch() throws Exception {
    // for issue #4622

    for (int i = 0; i < 100; i++) {
      db.begin();
      db.command("create vertex User set name = 'foo" + i + "'").close();
      db.commit();
    }

    db.begin();
    db.command("delete vertex User batch 5").close();
    db.commit();

    YTResultSet result = db.query("select from User");
    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test(expected = YTCommandExecutionException.class)
  public void testDeleteVertexWithEdgeRid() throws Exception {

    db.begin();
    db.command("create vertex User set name = 'foo1'").close();
    db.command("create vertex User set name = 'foo2'").close();
    db.command(
            "create edge E from (select from user where name = 'foo1') to (select from user where"
                + " name = 'foo2')")
        .close();
    db.commit();

    try (YTResultSet edges = db.query("select from e limit 1")) {
      db.begin();
      db.command("delete vertex [" + edges.next().getIdentity().get() + "]").close();
      db.commit();
      Assert.fail("Error on deleting a vertex with a rid of an edge");
    }
  }

  @Test
  public void testDeleteVertexFromSubquery() throws Exception {
    // for issue #4523

    for (int i = 0; i < 100; i++) {
      db.begin();
      db.command("create vertex User set name = 'foo" + i + "'").close();
      db.commit();
    }

    db.begin();
    db.command("delete vertex from (select from User)").close();
    db.commit();

    YTResultSet result = db.query("select from User");
    Assert.assertEquals(result.stream().count(), 0);
  }

  @Test
  public void testDeleteVertexFromSubquery2() throws Exception {
    // for issue #4523

    for (int i = 0; i < 100; i++) {
      db.begin();
      db.command("create vertex User set name = 'foo" + i + "'").close();
      db.commit();
    }

    db.begin();
    db.command("delete vertex from (select from User where name = 'foo10')").close();
    db.commit();

    YTResultSet result = db.query("select from User");
    Assert.assertEquals(result.stream().count(), 99);
  }
}
