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

import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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
    YTDocument singleLinked = new YTDocument();
    database.save(singleLinked);
    YTDocument doc = new YTDocument("FetchClass");
    doc.field("name", "first");
    database.save(doc);
    YTDocument doc1 = new YTDocument("FetchClass");
    doc1.field("name", "second");
    doc1.field("linked", singleLinked);
    database.save(doc1);
    YTDocument doc2 = new YTDocument("FetchClass");
    doc2.field("name", "third");
    List<YTDocument> linkList = new ArrayList<YTDocument>();
    linkList.add(doc);
    linkList.add(doc1);
    doc2.field("linkList", linkList);
    doc2.field("linked", singleLinked);
    Set<YTDocument> linkSet = new HashSet<YTDocument>();
    linkSet.add(doc);
    linkSet.add(doc1);
    doc2.field("linkSet", linkSet);
    database.save(doc2);

    YTDocument doc3 = new YTDocument("FetchClass");
    doc3.field("name", "forth");
    doc3.field("ref", doc2);
    doc3.field("linkSet", linkSet);
    doc3.field("linkList", linkList);
    database.save(doc3);

    YTDocument doc4 = new YTDocument("SecondFetchClass");
    doc4.field("name", "fifth");
    doc4.field("surname", "test");
    database.save(doc4);

    YTDocument doc5 = new YTDocument("SecondFetchClass");
    doc5.field("name", "sixth");
    doc5.field("surname", "test");
    database.save(doc5);

    YTDocument doc6 = new YTDocument("OutInFetchClass");
    ORidBag out = new ORidBag(database);
    out.add(doc2);
    out.add(doc3);
    doc6.field("out_friend", out);
    ORidBag in = new ORidBag(database);
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

    List<YTDocument> resultset =
        database.query(new OSQLSynchQuery<YTDocument>("select * from FetchClass"));
    Assert.assertEquals(YouTrackDBManager.instance().getProfiler().getCounter("Cache.reused"),
        times);

    YTRID linked;
    for (YTDocument d : resultset) {
      linked = d.field("linked", YTRID.class);
      if (linked != null) {
        Assert.assertNull(database.getLocalCache().findRecord(linked));
      }
    }
  }
}
