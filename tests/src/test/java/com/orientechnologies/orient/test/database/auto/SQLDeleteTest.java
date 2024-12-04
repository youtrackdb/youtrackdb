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

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "sql-delete")
public class SQLDeleteTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public SQLDeleteTest(boolean remote) {
    super(remote);
  }

  @Test
  public void deleteWithWhereOperator() {
    database.begin();
    database.command("insert into Profile (sex, salary) values ('female', 2100)").close();
    database.commit();

    final Long total = database.countClass("Profile");

    OResultSet resultset =
        database.query("select from Profile where sex = 'female' and salary = 2100");
    long queryCount = resultset.stream().count();

    database.begin();
    OResultSet result =
        database.command("delete from Profile where sex = 'female' and salary = 2100");
    database.commit();
    long count = result.next().getProperty("count");

    Assert.assertEquals(count, queryCount);

    Assert.assertEquals(database.countClass("Profile"), total - count);
  }

  @Test
  public void deleteInPool() {
    YTDatabaseSessionInternal db = acquireSession();

    final Long total = db.countClass("Profile");

    OResultSet resultset =
        db.query("select from Profile where sex = 'male' and salary > 120 and salary <= 133");

    long queryCount = resultset.stream().count();

    db.begin();
    OResultSet records =
        db.command("delete from Profile where sex = 'male' and salary > 120 and salary <= 133");
    db.commit();

    long count = records.next().getProperty("count");
    Assert.assertEquals(count, queryCount);

    Assert.assertEquals(db.countClass("Profile"), total - count);

    db.close();
  }
}
