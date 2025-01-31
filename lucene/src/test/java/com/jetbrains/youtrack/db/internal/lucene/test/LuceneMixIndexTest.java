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

package com.jetbrains.youtrack.db.internal.lucene.test;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.io.InputStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneMixIndexTest extends BaseLuceneTest {

  @Before
  public void initLocal() {

    var stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql");

    db.execute("sql", getScriptFromStream(stream)).close();

    db.command("create index Song.author on Song (author) NOTUNIQUE").close();

    db.command("create index Song.composite on Song (title,lyrics) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testMixQuery() {

    var docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and [title,lyrics]  LUCENE"
                + " \"(title:mountain)\" ");

    Assert.assertEquals(1, docs.stream().count());

    docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE"
                + " \"(title:mountain)\" ");

    Assert.assertEquals(1, docs.stream().count());

    docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE"
                + " \"(title:ballad)\" ");
    Assert.assertEquals(0, docs.stream().count());

    docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE"
                + " \"(title:ballad)\" ");
    Assert.assertEquals(0, docs.stream().count());
  }

  @Test
  //  @Ignore
  public void testMixCompositeQuery() {

    var docs =
        db.query(
            "select * from Song where  author = 'Hornsby' and [title,lyrics] LUCENE"
                + " \"title:mountain\" ");

    Assert.assertEquals(1, docs.stream().count());

    docs =
        db.query(
            "select * from Song where author = 'Hornsby' and [title,lyrics] LUCENE \"lyrics:happy\""
                + " ");

    Assert.assertEquals(1, docs.stream().count());

    // docs = databaseDocumentTx.query(new SQLSynchQuery<EntityImpl>(
    // "select * from Song where  author = 'Hornsby' and [title] LUCENE \"(title:ballad)\" "));
    // Assert.assertEquals(docs.size(), 0);
    //
    // docs = databaseDocumentTx.query(new SQLSynchQuery<EntityImpl>(
    // "select * from Song where  author = 'Hornsby' and title LUCENE \"(title:ballad)\" "));
    // Assert.assertEquals(docs.size(), 0);

  }
}
