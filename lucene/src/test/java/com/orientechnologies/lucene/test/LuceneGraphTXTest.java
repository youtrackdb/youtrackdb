/*
 *
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.orientechnologies.lucene.test;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneGraphTXTest extends BaseLuceneTest {

  @Before
  public void init() {
    YTClass type = db.createVertexClass("City");
    type.createProperty(db, "name", YTType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void graphTxTest() throws Exception {

    Vertex v = db.newVertex("City");
    v.setProperty("name", "London");

    db.begin();
    db.save(v);
    db.commit();

    db.begin();
    YTResultSet results = db.command("select from City where name lucene 'London'");
    Assert.assertEquals(results.stream().count(), 1);

    v = db.bindToSession(v);
    v.setProperty("name", "Berlin");

    v.save();
    db.commit();

    results = db.command("select from City where name lucene 'Berlin'");
    Assert.assertEquals(results.stream().count(), 1);

    results = db.command("select from City where name lucene 'London'");
    Assert.assertEquals(results.stream().count(), 0);

    // Assert After Commit
    results = db.command("select from City where name lucene 'Berlin'");
    Assert.assertEquals(results.stream().count(), 1);
    results = db.command("select from City where name lucene 'London'");
    Assert.assertEquals(results.stream().count(), 0);
  }
}
