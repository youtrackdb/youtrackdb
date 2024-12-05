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

import com.orientechnologies.core.YouTrackDBManager;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.query.OSQLSynchQuery;
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
public class QueryLocalCacheIntegrationTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public QueryLocalCacheIntegrationTest(boolean remote) {
    super(remote);
  }

  @BeforeMethod
  public void beforeMeth() throws Exception {
    database.getMetadata().getSchema().createClass("FetchClass");

    database
        .getMetadata()
        .getSchema()
        .createClass("SecondFetchClass")
        .createProperty(database, "surname", YTType.STRING)
        .setMandatory(database, true);
    database.getMetadata().getSchema().createClass("OutInFetchClass");

    database.begin();
    YTEntityImpl singleLinked = new YTEntityImpl();
    database.save(singleLinked);
    YTEntityImpl doc = new YTEntityImpl("FetchClass");
    doc.field("name", "first");
    database.save(doc);
    YTEntityImpl doc1 = new YTEntityImpl("FetchClass");
    doc1.field("name", "second");
    doc1.field("linked", singleLinked);
    database.save(doc1);
    YTEntityImpl doc2 = new YTEntityImpl("FetchClass");
    doc2.field("name", "third");
    List<YTEntityImpl> linkList = new ArrayList<YTEntityImpl>();
    linkList.add(doc);
    linkList.add(doc1);
    doc2.field("linkList", linkList);
    doc2.field("linked", singleLinked);
    Set<YTEntityImpl> linkSet = new HashSet<YTEntityImpl>();
    linkSet.add(doc);
    linkSet.add(doc1);
    doc2.field("linkSet", linkSet);
    database.save(doc2);

    YTEntityImpl doc3 = new YTEntityImpl("FetchClass");
    doc3.field("name", "forth");
    doc3.field("ref", doc2);
    doc3.field("linkSet", linkSet);
    doc3.field("linkList", linkList);
    database.save(doc3);

    YTEntityImpl doc4 = new YTEntityImpl("SecondFetchClass");
    doc4.field("name", "fifth");
    doc4.field("surname", "test");
    database.save(doc4);

    YTEntityImpl doc5 = new YTEntityImpl("SecondFetchClass");
    doc5.field("name", "sixth");
    doc5.field("surname", "test");
    database.save(doc5);

    YTEntityImpl doc6 = new YTEntityImpl("OutInFetchClass");
    RidBag out = new RidBag(database);
    out.add(doc2);
    out.add(doc3);
    doc6.field("out_friend", out);
    RidBag in = new RidBag(database);
    in.add(doc4);
    in.add(doc5);
    doc6.field("in_friend", in);
    doc6.field("name", "myName");
    database.save(doc6);

    database.commit();
  }

  @AfterMethod
  public void afterMeth() throws Exception {
    database.getMetadata().getSchema().dropClass("FetchClass");
    database.getMetadata().getSchema().dropClass("SecondFetchClass");
    database.getMetadata().getSchema().dropClass("OutInFetchClass");
  }

  @Test
  public void queryTest() {
    final long times = YouTrackDBManager.instance().getProfiler().getCounter("Cache.reused");

    List<YTEntityImpl> resultset =
        database.query(new OSQLSynchQuery<YTEntityImpl>("select * from FetchClass"));
    Assert.assertEquals(YouTrackDBManager.instance().getProfiler().getCounter("Cache.reused"),
        times);

    YTRID linked;
    for (YTEntityImpl d : resultset) {
      linked = d.field("linked", YTRID.class);
      if (linked != null) {
        Assert.assertNull(database.getLocalCache().findRecord(linked));
      }
    }
  }
}
