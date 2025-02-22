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
package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.session.SessionListener;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.internal.core.hook.DocumentHookAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * Tests the right calls of all the db's listener API.
 */
@Test
public class DbListenerTest extends BaseDBTest {

  protected int onAfterTxCommit = 0;
  protected int onAfterTxRollback = 0;
  protected int onBeforeTxBegin = 0;
  protected int onBeforeTxCommit = 0;
  protected int onBeforeTxRollback = 0;
  protected int onClose = 0;
  protected int onCreate = 0;
  protected int onDelete = 0;
  protected int onOpen = 0;
  protected int onCorruption = 0;
  protected String command;
  protected Object commandResult;

  public static class DocumentChangeListener {

    final Map<EntityImpl, List<String>> changes = new HashMap<EntityImpl, List<String>>();

    public DocumentChangeListener(final DatabaseSession db) {
      db.registerHook(
          new DocumentHookAbstract(db) {

            @Override
            public void onRecordAfterUpdate(EntityImpl entity) {
              List<String> changedFields = new ArrayList<>();
              Collections.addAll(changedFields, entity.getDirtyProperties());
              changes.put(entity, changedFields);
            }
          });
    }

    public Map<EntityImpl, List<String>> getChanges() {
      return changes;
    }
  }

  public class DbListener implements SessionListener {

    @Override
    public void onAfterTxCommit(DatabaseSession iDatabase) {
      onAfterTxCommit++;
    }

    @Override
    public void onAfterTxRollback(DatabaseSession iDatabase) {
      onAfterTxRollback++;
    }

    @Override
    public void onBeforeTxBegin(DatabaseSession iDatabase) {
      onBeforeTxBegin++;
    }

    @Override
    public void onBeforeTxCommit(DatabaseSession iDatabase) {
      onBeforeTxCommit++;
    }

    @Override
    public void onBeforeTxRollback(DatabaseSession iDatabase) {
      onBeforeTxRollback++;
    }

    @Override
    public void onClose(DatabaseSession iDatabase) {
      onClose++;
    }
  }

  @Parameters(value = "remote")
  public DbListenerTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testEmbeddedDbListeners() throws IOException {
    session = createSessionInstance();
    session.registerListener(new DbListener());

    final var baseOnBeforeTxBegin = onBeforeTxBegin;
    final var baseOnBeforeTxCommit = onBeforeTxCommit;
    final var baseOnAfterTxCommit = onAfterTxCommit;

    session.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 1);

    session
        .newInstance();

    session.commit();
    Assert.assertEquals(onBeforeTxCommit, baseOnBeforeTxCommit + 1);
    Assert.assertEquals(onAfterTxCommit, baseOnAfterTxCommit + 1);

    session.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 2);

    session.newInstance();

    session.rollback();
    Assert.assertEquals(onBeforeTxRollback, 1);
    Assert.assertEquals(onAfterTxRollback, 1);
  }

  @Test
  public void testRemoteDbListeners() throws IOException {
    if (!remoteDB) {
      return;
    }

    session = createSessionInstance();

    var listener = new DbListener();
    session.registerListener(listener);

    var baseOnBeforeTxBegin = onBeforeTxBegin;
    session.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 1);

    session
        .newInstance();

    var baseOnBeforeTxCommit = onBeforeTxCommit;
    var baseOnAfterTxCommit = onAfterTxCommit;
    session.commit();
    Assert.assertEquals(onBeforeTxCommit, baseOnBeforeTxCommit + 1);
    Assert.assertEquals(onAfterTxCommit, baseOnAfterTxCommit + 1);

    session.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 2);

    session
        .newInstance();

    var baseOnBeforeTxRollback = onBeforeTxRollback;
    var baseOnAfterTxRollback = onAfterTxRollback;
    session.rollback();
    Assert.assertEquals(onBeforeTxRollback, baseOnBeforeTxRollback + 1);
    Assert.assertEquals(onAfterTxRollback, baseOnAfterTxRollback + 1);

    var baseOnClose = onClose;
    session.close();
    Assert.assertEquals(onClose, baseOnClose + 1);
  }

  @Test
  public void testEmbeddedDbListenersTxRecords() throws IOException {
    if (remoteDB) {
      return;
    }
    session = createSessionInstance();

    session.begin();
    var rec =
        session
            .newInstance()
            .field("name", "Jay");

    session.commit();

    final var cl = new DocumentChangeListener(session);

    session.begin();
    rec = session.bindToSession(rec);
    rec.field("surname", "Miner");

    session.commit();

    Assert.assertEquals(cl.getChanges().size(), 1);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }

  @Test
  public void testEmbeddedDbListenersGraph() throws IOException {
    session = createSessionInstance();

    session.begin();
    var v = session.newVertex();
    v.setProperty("name", "Jay");

    session.commit();
    session.begin();
    final var cl = new DocumentChangeListener(session);

    v = session.bindToSession(v);
    v.setProperty("surname", "Miner");
    session.commit();
    session.close();

    Assert.assertEquals(cl.getChanges().size(), 1);
  }
}
