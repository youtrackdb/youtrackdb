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

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class QueryLocalCacheIntegrationTest extends BaseDBTest {

  @Parameters(value = "remote")
  public QueryLocalCacheIntegrationTest(boolean remote) {
    super(remote);
  }

  @BeforeMethod
  public void beforeMeth() throws Exception {
    db.getMetadata().getSchema().createClass("FetchClass");

    db
        .getMetadata()
        .getSchema()
        .createClass("SecondFetchClass")
        .createProperty(db, "surname", PropertyType.STRING)
        .setMandatory(db, true);
    db.getMetadata().getSchema().createClass("OutInFetchClass");

    db.begin();
    EntityImpl singleLinked = ((EntityImpl) db.newEntity());
    db.save(singleLinked);
    EntityImpl doc = ((EntityImpl) db.newEntity("FetchClass"));
    doc.field("name", "first");
    db.save(doc);
    EntityImpl doc1 = ((EntityImpl) db.newEntity("FetchClass"));
    doc1.field("name", "second");
    doc1.field("linked", singleLinked);
    db.save(doc1);
    EntityImpl doc2 = ((EntityImpl) db.newEntity("FetchClass"));
    doc2.field("name", "third");
    List<EntityImpl> linkList = new ArrayList<EntityImpl>();
    linkList.add(doc);
    linkList.add(doc1);
    doc2.field("linkList", linkList);
    doc2.field("linked", singleLinked);
    Set<EntityImpl> linkSet = new HashSet<EntityImpl>();
    linkSet.add(doc);
    linkSet.add(doc1);
    doc2.field("linkSet", linkSet);
    db.save(doc2);

    EntityImpl doc3 = ((EntityImpl) db.newEntity("FetchClass"));
    doc3.field("name", "forth");
    doc3.field("ref", doc2);
    doc3.field("linkSet", linkSet);
    doc3.field("linkList", linkList);
    db.save(doc3);

    EntityImpl doc4 = ((EntityImpl) db.newEntity("SecondFetchClass"));
    doc4.field("name", "fifth");
    doc4.field("surname", "test");
    db.save(doc4);

    EntityImpl doc5 = ((EntityImpl) db.newEntity("SecondFetchClass"));
    doc5.field("name", "sixth");
    doc5.field("surname", "test");
    db.save(doc5);

    EntityImpl doc6 = ((EntityImpl) db.newEntity("OutInFetchClass"));
    RidBag out = new RidBag(db);
    out.add(doc2);
    out.add(doc3);
    doc6.field("out_friend", out);
    RidBag in = new RidBag(db);
    in.add(doc4);
    in.add(doc5);
    doc6.field("in_friend", in);
    doc6.field("name", "myName");
    db.save(doc6);

    db.commit();
  }

  @AfterMethod
  public void afterMeth() throws Exception {
    db.getMetadata().getSchema().dropClass("FetchClass");
    db.getMetadata().getSchema().dropClass("SecondFetchClass");
    db.getMetadata().getSchema().dropClass("OutInFetchClass");
  }

  @Test
  public void queryTest() {
    final long times = YouTrackDBEnginesManager.instance().getProfiler().getCounter("Cache.reused");

    List<EntityImpl> resultset =
        db.query(new SQLSynchQuery<EntityImpl>("select * from FetchClass"));
    Assert.assertEquals(
        YouTrackDBEnginesManager.instance().getProfiler().getCounter("Cache.reused"),
        times);

    RID linked;
    for (EntityImpl d : resultset) {
      linked = d.field("linked", RID.class);
      if (linked != null) {
        Assert.assertNull(db.getLocalCache().findRecord(linked));
      }
    }
  }
}
