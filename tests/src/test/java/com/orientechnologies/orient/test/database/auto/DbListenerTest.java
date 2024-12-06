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

import com.jetbrains.youtrack.db.internal.core.command.CommandExecutor;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilder;
import com.jetbrains.youtrack.db.internal.core.hook.RecordHook;
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
public class DbListenerTest extends DocumentDBBaseTest {

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
            public RecordHook.DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
              return RecordHook.DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
            }

            @Override
            public void onRecordAfterUpdate(EntityImpl iDocument) {
              List<String> changedFields = new ArrayList<>();
              Collections.addAll(changedFields, iDocument.getDirtyFields());
              changes.put(iDocument, changedFields);
            }
          });
    }

    public Map<EntityImpl, List<String>> getChanges() {
      return changes;
    }
  }

  public class DbListener implements DatabaseListener {

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

    @Override
    public void onBeforeCommand(CommandRequestText iCommand, CommandExecutor executor) {
      command = iCommand.getText();
    }

    @Override
    public void onAfterCommand(
        CommandRequestText iCommand, CommandExecutor executor, Object result) {
      commandResult = result;
    }

    @Override
    public void onCreate(DatabaseSession iDatabase) {
      onCreate++;
    }

    @Override
    public void onDelete(DatabaseSession iDatabase) {
      onDelete++;
    }

    @Override
    public void onOpen(DatabaseSession iDatabase) {
      onOpen++;
    }

    @Override
    public boolean onCorruptionRepairDatabase(
        DatabaseSession iDatabase, final String iReason, String iWhatWillbeFixed) {
      onCorruption++;
      return true;
    }
  }

  @Parameters(value = "remote")
  public DbListenerTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @Test
  public void testEmbeddedDbListeners() throws IOException {
    database = createSessionInstance();
    database.registerListener(new DbListener());

    final int baseOnBeforeTxBegin = onBeforeTxBegin;
    final int baseOnBeforeTxCommit = onBeforeTxCommit;
    final int baseOnAfterTxCommit = onAfterTxCommit;

    database.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 1);

    database
        .<EntityImpl>newInstance().save();
    database.commit();
    Assert.assertEquals(onBeforeTxCommit, baseOnBeforeTxCommit + 1);
    Assert.assertEquals(onAfterTxCommit, baseOnAfterTxCommit + 1);

    database.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 2);

    database.<EntityImpl>newInstance().save();
    database.rollback();
    Assert.assertEquals(onBeforeTxRollback, 1);
    Assert.assertEquals(onAfterTxRollback, 1);
  }

  @Test
  public void testRemoteDbListeners() throws IOException {
    if (!remoteDB) {
      return;
    }

    database = createSessionInstance();

    var listener = new DbListener();
    database.registerListener(listener);

    var baseOnBeforeTxBegin = onBeforeTxBegin;
    database.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 1);

    database
        .<EntityImpl>newInstance()
        .save();
    var baseOnBeforeTxCommit = onBeforeTxCommit;
    var baseOnAfterTxCommit = onAfterTxCommit;
    database.commit();
    Assert.assertEquals(onBeforeTxCommit, baseOnBeforeTxCommit + 1);
    Assert.assertEquals(onAfterTxCommit, baseOnAfterTxCommit + 1);

    database.begin();
    Assert.assertEquals(onBeforeTxBegin, baseOnBeforeTxBegin + 2);

    database
        .<EntityImpl>newInstance()
        .save();
    var baseOnBeforeTxRollback = onBeforeTxRollback;
    var baseOnAfterTxRollback = onAfterTxRollback;
    database.rollback();
    Assert.assertEquals(onBeforeTxRollback, baseOnBeforeTxRollback + 1);
    Assert.assertEquals(onAfterTxRollback, baseOnAfterTxRollback + 1);

    var baseOnClose = onClose;
    database.close();
    Assert.assertEquals(onClose, baseOnClose + 1);
  }

  @Test
  public void testEmbeddedDbListenersTxRecords() throws IOException {
    if (remoteDB) {
      return;
    }
    database = createSessionInstance();

    database.begin();
    EntityImpl rec =
        database
            .<EntityImpl>newInstance()
            .field("name", "Jay");
    rec.save();
    database.commit();

    final DocumentChangeListener cl = new DocumentChangeListener(database);

    database.begin();
    rec = database.bindToSession(rec);
    rec.field("surname", "Miner").save();
    database.commit();

    Assert.assertEquals(cl.getChanges().size(), 1);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilder builder) {
    builder.addConfig(GlobalConfiguration.NON_TX_READS_WARNING_MODE, "EXCEPTION");
    return builder.build();
  }

  @Test
  public void testEmbeddedDbListenersGraph() throws IOException {
    database = createSessionInstance();

    database.begin();
    var v = database.newVertex();
    v.setProperty("name", "Jay");
    v.save();

    database.commit();
    database.begin();
    final DocumentChangeListener cl = new DocumentChangeListener(database);

    v = database.bindToSession(v);
    v.setProperty("surname", "Miner");
    v.save();
    database.commit();
    database.close();

    Assert.assertEquals(cl.getChanges().size(), 1);
  }
}
