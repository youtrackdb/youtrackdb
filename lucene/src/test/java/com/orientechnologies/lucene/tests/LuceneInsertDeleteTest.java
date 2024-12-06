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

package com.orientechnologies.lucene.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
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
public class LuceneInsertDeleteTest extends LuceneBaseTest {

  @Before
  public void init() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", PropertyType.STRING);
    //noinspection EmptyTryBlock
    try (ResultSet resultSet =
        db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE")) {
    }
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

    Index idx = schema.getClass("City").getClassIndex(db, "City.name");
    Collection<?> coll;
    try (Stream<RID> stream = idx.getInternal().getRids(db, "Rome")) {
      coll = stream.collect(Collectors.toList());
    }

    db.begin();
    assertThat(coll).hasSize(1);
    assertThat(idx.getInternal().size(db)).isEqualTo(1);

    Identifiable next = (Identifiable) coll.iterator().next();
    doc = db.load(next.getIdentity());

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
      //noinspection EmptyTryBlock
      try (ResultSet resultSet = db.execute("sql", getScriptFromStream(stream))) {
      }
    }

    //noinspection EmptyTryBlock
    try (ResultSet resultSet =
        db.command(
            "create index Song.title on Song (title) FULLTEXT ENGINE LUCENE metadata"
                + " {'closeAfterInterval':1000 , 'firstFlushAfter':1000 }")) {
    }

    try (ResultSet docs = db.query("select from Song where title lucene 'mountain'")) {

      assertThat(docs).hasSize(4);
      TimeUnit.SECONDS.sleep(5);
      docs.close();

      db.begin();
      //noinspection EmptyTryBlock
      try (ResultSet command =
          db.command("delete vertex from Song where title lucene 'mountain'")) {
      }
      db.commit();

      try (ResultSet resultSet = db.query("select from Song where  title lucene 'mountain'")) {
        assertThat(resultSet).hasSize(0);
      }
    }
  }
}
