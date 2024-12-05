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

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Ignore;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class SQLSelectGroupByTest extends DocumentDBBaseTest {
  @Parameters(value = "remote")
  public SQLSelectGroupByTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    super.beforeClass();
    generateCompanyData();
  }

  @Test(enabled = false)
  public void queryGroupByBasic() {
    List<EntityImpl> result = executeQuery("select location from Account group by location");

    Assert.assertTrue(result.size() > 1);
    Set<Object> set = new HashSet<Object>();
    for (EntityImpl d : result) {
      set.add(d.field("location"));
    }
    Assert.assertEquals(result.size(), set.size());
  }

  @Test
  public void queryGroupByLimit() {
    List<EntityImpl> result =
        executeQuery("select location from Account group by location limit 2");

    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void queryGroupByCount() {
    List<EntityImpl> result =
        executeQuery("select count(*) from Account group by location");

    Assert.assertTrue(result.size() > 1);
  }

  @Test
  @Ignore
  public void queryGroupByAndOrderBy() {
    List<EntityImpl> result =
        executeQuery("select location from Account group by location order by location");

    Assert.assertTrue(result.size() > 1);
    String last = null;
    for (EntityImpl d : result) {
      if (last != null) {
        Assert.assertTrue(last.compareTo(d.field("location")) < 0);
      }
      last = d.field("location");
    }

    result = executeQuery("select location from Account group by location order by location desc");

    Assert.assertTrue(result.size() > 1);
    last = null;
    for (EntityImpl d : result) {
      Object current = d.field("location");
      if (current != null) {
        if (last != null) {
          Assert.assertTrue(last.compareTo((String) current) > 0);
        }
      }
      last = d.field("location");
    }
  }

  @Test
  public void queryGroupByAndWithNulls() {
    // INSERT WITH NO LOCATION (AS NULL)
    database.command("create class GroupByTest extends V").close();
    try {
      database.begin();
      database.command("insert into GroupByTest set testNull = true").close();
      database.command("insert into GroupByTest set location = 'Rome'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();
      database.commit();

      final List<EntityImpl> result =
          executeQuery(
              "select location, count(*) from GroupByTest group by location");

      Assert.assertEquals(result.size(), 3);

      boolean foundNullGroup = false;
      for (EntityImpl d : result) {
        if (d.field("location") == null) {
          Assert.assertFalse(foundNullGroup);
          foundNullGroup = true;
        }
      }

      Assert.assertTrue(foundNullGroup);
    } finally {
      database.begin();
      database.command("delete vertex GroupByTest").close();
      database.commit();

      database.command("drop class GroupByTest UNSAFE").close();
    }
  }

  @Test
  public void queryGroupByNoNulls() {
    database.command("create class GroupByTest extends V").close();
    try {
      database.begin();
      database.command("insert into GroupByTest set location = 'Rome'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();
      database.command("insert into GroupByTest set location = 'Austin'").close();
      database.commit();

      final List<EntityImpl> result = executeQuery(
          "select location, count(*) from GroupByTest group by location");

      Assert.assertEquals(result.size(), 2);

      for (EntityImpl d : result) {
        Assert.assertNotNull(d.field("location"), "Found null in resultset with groupby");
      }

    } finally {
      database.begin();
      database.command("delete vertex GroupByTest").close();
      database.commit();

      database.command("drop class GroupByTest UNSAFE").close();
    }
  }
}
