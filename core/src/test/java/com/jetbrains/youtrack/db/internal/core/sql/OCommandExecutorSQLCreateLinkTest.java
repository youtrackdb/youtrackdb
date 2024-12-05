/*
 *
 *  *  Copyright YouTrackDB
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
package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class OCommandExecutorSQLCreateLinkTest extends DBTestBase {

  @Test
  public void testBasic() {
    var basic1 = db.getMetadata().getSchema().createClass("Basic1");
    basic1.createProperty(db, "theLink", YTType.LINK);

    var basic2 = db.getMetadata().getSchema().createClass("Basic2");
    basic2.createProperty(db, "theLink", YTType.LINK);

    db.begin();
    db.command("insert into Basic1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Basic1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();

    db.command("insert into Basic2 set pk = 'pkb2_1'").close();
    db.command("insert into Basic2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink type link FROM Basic1.fk TO Basic2.pk ").close();
    db.commit();

    YTResultSet result = db.query("select pk, theLink.pk as other from Basic1 order by pk");

    Object otherKey = result.next().getProperty("other");
    Assert.assertNotNull(otherKey);

    Assert.assertEquals(otherKey, "pkb2_1");

    otherKey = result.next().getProperty("other");
    Assert.assertEquals(otherKey, "pkb2_2");
  }

  @Test
  public void testInverse() {
    var inverse1 = db.getMetadata().getSchema().createClass("Inverse1");
    inverse1.createProperty(db, "theLink", YTType.LINK);

    var inverse2 = db.getMetadata().getSchema().createClass("Inverse2");
    inverse2.createProperty(db, "theLink", YTType.LINKSET);

    db.begin();
    db.command("insert into Inverse1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_3', fk = 'pkb2_2'").close();

    db.command("insert into Inverse2 set pk = 'pkb2_1'").close();
    db.command("insert into Inverse2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink TYPE LINKSET FROM Inverse1.fk TO Inverse2.pk INVERSE").close();
    db.commit();

    YTResultSet result = db.query("select pk, theLink.pk as other from Inverse2 order by pk");
    YTResult first = result.next();
    Object otherKeys = first.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).get(0), "pkb1_1");

    YTResult second = result.next();
    otherKeys = second.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).size(), 2);
    Assert.assertTrue(((List) otherKeys).contains("pkb1_2"));
    Assert.assertTrue(((List) otherKeys).contains("pkb1_3"));
  }
}
