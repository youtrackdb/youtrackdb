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

package com.jetbrains.youtrack.db.internal.lucene.tests;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import org.assertj.core.api.Assertions;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneMassiveInsertDeleteTest extends LuceneBaseTest {

  @Before
  public void init() {
    Schema schema = session.getMetadata().getSchema();
    var song = session.createVertexClass("City");
    song.createProperty(session, "name", PropertyType.STRING);

    session.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void loadCloseDelete() {

    var size = 1000;
    for (var i = 0; i < size; i++) {
      var city = session.newVertex("City");
      city.setProperty("name", "Rome " + i);

      session.begin();
      session.commit();
    }
    var query = "select * from City where search_class('name:Rome')=true";
    var docs = session.query(query);
    Assertions.assertThat(docs).hasSize(size);
    docs.close();
    session.close();

    session = (DatabaseSessionInternal) pool.acquire();
    docs = session.query(query);
    Assertions.assertThat(docs).hasSize(size);
    docs.close();

    session.begin();
    session.command("delete vertex City");
    session.commit();

    docs = session.query(query);
    Assertions.assertThat(docs).hasSize(0);
    docs.close();
    session.close();
    session = (DatabaseSessionInternal) pool.acquire();
    docs = session.query(query);
    Assertions.assertThat(docs).hasSize(0);
    docs.close();
    session.getMetadata().reload();

    session.begin();
    var idx = session.getMetadata().getSchema().getClassInternal("City")
        .getClassIndex(session, "City.name");
    Assert.assertEquals(0, idx.getInternal().size(session));
    session.commit();
  }
}
