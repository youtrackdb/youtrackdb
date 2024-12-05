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

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneInsertReadMultiThreadTest extends OLuceneBaseTest {

  private static final int THREADS = 10;
  private static final int RTHREADS = 10;
  private static final int CYCLE = 100;

  @Before
  public void init() {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", YTType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {

    List<CompletableFuture<Void>> futures =
        IntStream.range(0, THREADS)
            .boxed()
            .map(i -> CompletableFuture.runAsync(new LuceneInsert(pool, CYCLE)))
            .collect(Collectors.toList());

    futures.addAll(
        IntStream.range(0, 1)
            .boxed()
            .map(i -> CompletableFuture.runAsync(new LuceneReader(pool, CYCLE)))
            .collect(Collectors.toList()));

    futures.forEach(cf -> cf.join());

    YTDatabaseSessionInternal db1 = (YTDatabaseSessionInternal) pool.acquire();
    db1.getMetadata().reload();
    YTSchema schema = db1.getMetadata().getSchema();

    OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");

    db1.begin();
    Assert.assertEquals(idx.getInternal().size(db1), THREADS * CYCLE);
    db1.commit();
  }

  public class LuceneInsert implements Runnable {

    private final ODatabasePool pool;
    private final int cycle;
    private final int commitBuf;

    public LuceneInsert(ODatabasePool pool, int cycle) {
      this.pool = pool;
      this.cycle = cycle;

      this.commitBuf = cycle / 10;
    }

    @Override
    public void run() {

      final YTDatabaseSession db = pool.acquire();
      db.activateOnCurrentThread();
      db.begin();
      int i = 0;
      for (; i < cycle; i++) {
        YTEntity doc = db.newEntity("City");

        doc.setProperty("name", "Rome");

        db.save(doc);
        if (i % commitBuf == 0) {
          db.commit();
          db.begin();
        }
      }
      db.commit();
      db.close();
    }
  }

  public class LuceneReader implements Runnable {

    private final int cycle;
    private final ODatabasePool pool;

    public LuceneReader(ODatabasePool pool, int cycle) {
      this.pool = pool;
      this.cycle = cycle;
    }

    @Override
    public void run() {

      final YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) pool.acquire();
      db.activateOnCurrentThread();
      YTSchema schema = db.getMetadata().getSchema();
      OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");

      for (int i = 0; i < cycle; i++) {

        YTResultSet resultSet =
            db.query("select from City where SEARCH_FIELDS(['name'], 'Rome') =true ");

        if (resultSet.hasNext()) {
          assertThat(resultSet.next().toEntity().<String>getProperty("name"))
              .isEqualToIgnoringCase("rome");
        }
        resultSet.close();
      }
      db.close();
    }
  }
}
