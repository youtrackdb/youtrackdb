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

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
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

    Schema schema = db.getMetadata().getSchema();
    SchemaClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", PropertyType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testInsertUpdateWithIndex() {

    db.getMetadata().reload();
    Schema schema = db.getMetadata().getSchema();

    EntityImpl doc = new EntityImpl("City");
    doc.field("name", "Rome");
    db.begin();
    db.save(doc);
    db.commit();

    Index idx = db.getClassInternal("City").getClassIndex(db, "City.name");
    Collection<?> coll;
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }

    db.begin();
    assertThat(coll).hasSize(1);
    assertThat(idx.getInternal().size(db)).isEqualTo(1);
    db.commit();

    Identifiable next = (Identifiable) coll.iterator().next();
    doc = db.load(next.getIdentity());

    db.begin();
    doc = db.bindToSession(doc);
    db.delete(doc);
    db.commit();

    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
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

    ResultSet docs = db.query("select from Song where title lucene 'mountain'");

    assertThat(docs).hasSize(4);
    TimeUnit.SECONDS.sleep(5);

    db.begin();
    db.command("delete vertex from Song where title lucene 'mountain'").close();
    db.commit();

    docs = db.query("select from Song where  title lucene 'mountain'");
    assertThat(docs).hasSize(0);
  }
}
