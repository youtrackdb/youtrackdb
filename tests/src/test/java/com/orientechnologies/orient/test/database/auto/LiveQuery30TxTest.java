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

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.util.OPair;
import com.jetbrains.youtrack.db.internal.core.command.OCommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTLiveQueryMonitor;
import com.jetbrains.youtrack.db.internal.core.db.YTLiveQueryResultListener;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * only for remote usage (it requires registered LiveQuery plugin)
 */
@Test(groups = "Query")
public class LiveQuery30TxTest extends DocumentDBBaseTest implements OCommandOutputListener {

  private final CountDownLatch latch = new CountDownLatch(2);
  private final CountDownLatch unLatch = new CountDownLatch(1);

  class MyLiveQueryListener implements YTLiveQueryResultListener {

    public List<OPair<String, YTResult>> ops = new ArrayList<>();
    public int unsubscribe;

    @Override
    public void onCreate(YTDatabaseSession database, YTResult data) {
      ops.add(new OPair<>("create", data));
      latch.countDown();
    }

    @Override
    public void onUpdate(YTDatabaseSession database, YTResult before, YTResult after) {
      ops.add(new OPair<>("update", after));
      latch.countDown();
    }

    @Override
    public void onDelete(YTDatabaseSession database, YTResult data) {
      ops.add(new OPair<>("delete", data));
      latch.countDown();
    }

    @Override
    public void onError(YTDatabaseSession database, YTException exception) {
    }

    @Override
    public void onEnd(YTDatabaseSession database) {
      unsubscribe = 1;
      unLatch.countDown();
    }
  }

  @Parameters(value = {"remote"})
  public LiveQuery30TxTest(boolean remote) {
    super(remote);
  }

  @Test
  public void checkLiveQueryTx() throws IOException, InterruptedException {
    final String className1 = "LiveQuery30Test_checkLiveQueryTx_1";
    final String className2 = "LiveQuery30Test_checkLiveQueryTx_2";
    database.getMetadata().getSchema().createClass(className1);
    database.getMetadata().getSchema().createClass(className2);

    MyLiveQueryListener listener = new MyLiveQueryListener();

    YTLiveQueryMonitor monitor = database.live("live select from " + className1, listener);
    Assert.assertNotNull(monitor);
    database.begin();
    database.command("insert into " + className1 + " set name = 'foo', surname = 'bar'");
    database.command("insert into  " + className1 + " set name = 'foo', surname = 'baz'");
    database.command("insert into " + className2 + " set name = 'foo'");
    database.commit();
    latch.await(1, TimeUnit.MINUTES);

    monitor.unSubscribe();
    database.command("insert into " + className1 + " set name = 'foo', surname = 'bax'");
    Assert.assertEquals(listener.ops.size(), 2);
    for (OPair doc : listener.ops) {
      Assert.assertEquals(doc.getKey(), "create");
      YTResult res = (YTResult) doc.getValue();
      Assert.assertEquals((res).getProperty("name"), "foo");
      Assert.assertNotNull(res.getProperty("@rid"));
      Assert.assertTrue(((YTRID) res.getProperty("@rid")).getClusterPosition() >= 0);
    }
    unLatch.await(1, TimeUnit.MINUTES);
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    // System.out.print(iText);
    // System.out.flush();
  }
}
