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

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.InputStream;
import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneInsertDeleteTest extends BaseLuceneTest {

  @Before
  public void init() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", YTType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testInsertUpdateWithIndex() {

    db.getMetadata().reload();
    YTSchema schema = db.getMetadata().getSchema();

    EntityImpl doc = new EntityImpl("City");
    doc.field("name", "Rome");
    db.begin();
    db.save(doc);
    db.commit();

    OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");
    Collection<?> coll;
    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }

    db.begin();
    assertThat(coll).hasSize(1);
    assertThat(idx.getInternal().size(db)).isEqualTo(1);
    db.commit();

    YTIdentifiable next = (YTIdentifiable) coll.iterator().next();
    doc = db.load(next.getIdentity());

    db.begin();
    doc = db.bindToSession(doc);
    db.delete(doc);
    db.commit();

    try (Stream<YTRID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }
    assertThat(coll).hasSize(0);
    assertThat(idx.getInternal().size(db)).isEqualTo(0);
  }

  @Test
  public void testDeleteWithQueryOnClosedIndex() throws Exception {

    try (InputStream stream = ClassLoader.getSystemResourceAsStream("testLuceneIndex.sql")) {
      db.execute("sql", getScriptFromStream(stream)).close();
    }

    db.command(
            "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata"
                + " {'closeAfterInterval':1000 , 'firstFlushAfter':1000 }")
        .close();

    YTResultSet docs = db.query("select from Song where title lucene 'mountain'");

    assertThat(docs).hasSize(4);
    TimeUnit.SECONDS.sleep(5);

    db.begin();
    db.command("delete vertex from Song where title lucene 'mountain'").close();
    db.commit();

    docs = db.query("select from Song where  title lucene 'mountain'");
    assertThat(docs).hasSize(0);
  }
}
