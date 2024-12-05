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

import com.jetbrains.youtrack.db.internal.core.command.OCommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.IOException;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class DbCopyTest extends DocumentDBBaseTest implements OCommandOutputListener {

  @Parameters(value = {"remote"})
  public DbCopyTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    return builder.build();
  }

  @Test
  public void checkCopy() throws IOException {
    final String className = "DbCopyTest";
    database.getMetadata().getSchema().createClass(className);

    Thread thread =
        new Thread() {
          @Override
          public void run() {
            final YTDatabaseSessionInternal otherDB = database.copy();
            otherDB.activateOnCurrentThread();
            for (int i = 0; i < 5; i++) {
              otherDB.begin();
              EntityImpl doc = otherDB.newInstance(className);
              doc.field("num", i);
              doc.save();
              otherDB.commit();
              try {
                Thread.sleep(10);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            }
            otherDB.close();
          }
        };
    thread.start();

    for (int i = 0; i < 20; i++) {
      database.begin();
      EntityImpl doc = database.newInstance(className);
      doc.field("num", i);
      doc.save();
      database.commit();
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    try {
      thread.join();
    } catch (InterruptedException e) {
      Assert.fail();
    }

    database.begin();
    YTResultSet result = database.query("SELECT FROM " + className);
    Assert.assertEquals(result.stream().count(), 25);
    database.commit();
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    // System.out.print(iText);
    // System.out.flush();
  }
}
