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

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneMassiveInsertDeleteTest extends BaseLuceneTest {

  public LuceneMassiveInsertDeleteTest() {
  }

  @Before
  public void init() {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass v = schema.getClass("V");
    YTClass song = schema.createClass("City");
    song.addSuperClass(db, v);
    song.createProperty(db, "name", YTType.STRING);

    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void loadCloseDelete() {

    int size = 1000;
    for (int i = 0; i < size; i++) {
      YTDocument city = new YTDocument("City");
      city.field("name", "Rome " + i);

      db.begin();
      db.save(city);
      db.commit();
    }
    String query = "select * from City where name LUCENE 'name:Rome'";
    OResultSet docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), size);

    db.close();
    db = (YTDatabaseSessionInternal) openDatabase();

    docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), size);

    db.begin();
    db.command("delete vertex City").close();
    db.commit();

    docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), 0);

    db.close();
    db = (YTDatabaseSessionInternal) openDatabase();
    docs = db.query(query);
    Assert.assertEquals(docs.stream().count(), 0);

    db.getMetadata().reload();

    db.begin();
    OIndex idx = db.getMetadata().getSchema().getClass("City").getClassIndex(db, "City.name");
    Assert.assertEquals(idx.getInternal().size(db), 0);
    db.commit();
  }
}
