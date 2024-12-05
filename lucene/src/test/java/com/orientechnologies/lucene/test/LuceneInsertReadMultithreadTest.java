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

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneInsertReadMultithreadTest extends BaseLuceneTest {

  private static final int THREADS = 10;
  private static final int RTHREADS = 1;
  private static final int CYCLE = 100;

  protected String url = "";

  @Before
  public void init() {

    url = db.getURL();
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("City");

    oClass.createProperty(db, "name", YTType.STRING);
    db.command("create index City.name on City (name) FULLTEXT ENGINE LUCENE").close();
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {

    db.getMetadata().reload();
    YTSchema schema = db.getMetadata().getSchema();

    Thread[] threads = new Thread[THREADS + RTHREADS];
    for (int i = 0; i < THREADS; ++i) {
      threads[i] = new Thread(new LuceneInsertThread(CYCLE), "ConcurrentWriteTest" + i);
    }

    for (int i = THREADS; i < THREADS + RTHREADS; ++i) {
      threads[i] = new Thread(new LuceneReadThread(CYCLE), "ConcurrentReadTest" + i);
    }

    for (int i = 0; i < THREADS + RTHREADS; ++i) {
      threads[i].start();
    }

    System.out.println(
        "Started LuceneInsertReadMultithreadBaseTest test, waiting for "
            + threads.length
            + " threads to complete...");

    for (int i = 0; i < THREADS + RTHREADS; ++i) {
      threads[i].join();
    }

    System.out.println("LuceneInsertReadMultithreadBaseTest all threads completed");

    OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");

    db.begin();
    Assert.assertEquals(idx.getInternal().size(db), THREADS * CYCLE);
    db.commit();
  }

  public class LuceneInsertThread implements Runnable {

    private YTDatabaseSession db;
    private int cycle = 0;
    private final int commitBuf = 500;

    public LuceneInsertThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      db = openDatabase();

      db.begin();
      for (int i = 0; i < cycle; i++) {
        YTEntityImpl doc = new YTEntityImpl("City");

        doc.field("name", "Rome");

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

  public class LuceneReadThread implements Runnable {

    private final int cycle;
    private YTDatabaseSessionInternal databaseDocumentTx;

    public LuceneReadThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      databaseDocumentTx = openDatabase();

      YTSchema schema = databaseDocumentTx.getMetadata().getSchema();
      OIndex idx = schema.getClass("City").getClassIndex(db, "City.name");

      for (int i = 0; i < cycle; i++) {

        databaseDocumentTx.query("select from city where name LUCENE 'Rome'");
      }
    }
  }
}
