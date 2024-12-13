/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigBuilderImpl;
import com.jetbrains.youtrack.db.api.record.RecordHookAbstract;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class HookTxTest extends DocumentDBBaseTest {

  public static final int RECORD_BEFORE_CREATE = 3;
  public static final int RECORD_AFTER_CREATE = 5;
  public static final int RECORD_BEFORE_READ = 7;
  public static final int RECORD_AFTER_READ = 11;
  public static final int RECORD_BEFORE_UPDATE = 13;
  public static final int RECORD_AFTER_UPDATE = 17;
  public static final int RECORD_BEFORE_DELETE = 19;
  public static final int RECORD_AFTER_DELETE = 23;

  private int callbackCount = 0;
  private Entity profile;
  private int expectedHookState;

  private final class RecordHook extends RecordHookAbstract {

    @Override
    @Test(enabled = false)
    public RESULT onRecordBeforeCreate(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_BEFORE_CREATE;
      }
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    @Test(enabled = false)
    public void onRecordAfterCreate(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_AFTER_CREATE;
      }
    }

    @Override
    @Test(enabled = false)
    public RESULT onRecordBeforeRead(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_BEFORE_READ;
      }
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    @Test(enabled = false)
    public void onRecordAfterRead(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_AFTER_READ;
      }
    }

    @Override
    @Test(enabled = false)
    public RESULT onRecordBeforeUpdate(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_BEFORE_UPDATE;
      }
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    @Test(enabled = false)
    public void onRecordAfterUpdate(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_AFTER_UPDATE;
      }
    }

    @Override
    @Test(enabled = false)
    public RESULT onRecordBeforeDelete(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_BEFORE_DELETE;
      }
      return RESULT.RECORD_NOT_CHANGED;
    }

    @Override
    @Test(enabled = false)
    public void onRecordAfterDelete(Record iRecord) {
      if (iRecord instanceof EntityImpl
          && ((EntityImpl) iRecord).getClassName() != null
          && ((EntityImpl) iRecord).getClassName().equals("Profile")) {
        callbackCount += RECORD_AFTER_DELETE;
      }
    }

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.TARGET_NODE;
    }
  }

  @Parameters(value = "remote")
  public HookTxTest(@Optional Boolean remote) {
    super(remote != null ? remote : false);
  }

  @Override
  protected YouTrackDBConfig createConfig(YouTrackDBConfigBuilderImpl builder) {
    builder.addGlobalConfigurationParameter(GlobalConfiguration.NON_TX_READS_WARNING_MODE,
        "EXCEPTION");
    return builder.build();
  }


  @Test
  public void testRegisterHook() {
    database.registerHook(new RecordHook());
  }

  @Test(dependsOnMethods = "testRegisterHook")
  public void testHookCallsCreate() {
    database.registerHook(new RecordHook());
    profile = database.newInstance("Profile");
    profile.setProperty("nick", "HookTxTest");
    profile.setProperty("value", 0);

    expectedHookState = 0;

    // TEST HOOKS ON CREATE
    Assert.assertEquals(callbackCount, 0);
    database.begin();
    database.save(profile);
    database.commit();

    expectedHookState += RECORD_BEFORE_CREATE + RECORD_AFTER_CREATE;
    Assert.assertEquals(callbackCount, expectedHookState);
  }

  @Test(dependsOnMethods = "testHookCallsCreate")
  public void testHookCallsRead() {
    database.registerHook(new RecordHook());
    // TEST HOOKS ON READ
    database.begin();

    expectedHookState += RECORD_BEFORE_READ + RECORD_AFTER_READ;

    this.profile = database.load(profile.getIdentity());
    Assert.assertEquals(callbackCount, expectedHookState);

    Assert.assertEquals(callbackCount, expectedHookState);
    database.commit();
  }

  @Test(dependsOnMethods = "testHookCallsRead")
  public void testHookCallsUpdate() {
    database.registerHook(new RecordHook());
    database.begin();
    profile = database.load(profile.getIdentity());
    // TEST HOOKS ON UPDATE
    profile.setProperty("value", profile.<Integer>getProperty("value") + 1000);
    database.save(profile);

    database.commit();

    expectedHookState +=
        RECORD_BEFORE_UPDATE + RECORD_AFTER_UPDATE + RECORD_BEFORE_READ + RECORD_AFTER_READ;
    Assert.assertEquals(callbackCount, expectedHookState);
  }

  @Test(dependsOnMethods = "testHookCallsUpdate")
  public void testHookCallsDelete() {
    database.registerHook(new RecordHook());
    // TEST HOOKS ON DELETE
    database.begin();
    database.delete(database.bindToSession(profile));
    database.commit();

    expectedHookState +=
        RECORD_BEFORE_DELETE + RECORD_AFTER_DELETE + RECORD_BEFORE_READ + RECORD_AFTER_READ;
    Assert.assertEquals(callbackCount, expectedHookState);

    database.unregisterHook(new RecordHook());
  }
}
