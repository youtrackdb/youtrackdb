package com.jetbrains.youtrack.db.internal.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.hook.YTRecordHook;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityPolicy;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Test;

public class HookReadTest extends DBTestBase {

  @Test
  public void testSelectChangedInHook() {
    db.registerHook(
        new YTRecordHook() {
          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(TYPE iType, Record iRecord) {
            if (iType == TYPE.AFTER_READ
                && !((EntityImpl) iRecord)
                .getClassName()
                .equalsIgnoreCase(OSecurityPolicy.class.getSimpleName())) {
              ((EntityImpl) iRecord).field("read", "test");
            }
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        });

    db.getMetadata().getSchema().createClass("TestClass");
    db.begin();
    db.save(new EntityImpl("TestClass"));
    db.commit();

    db.begin();
    YTResultSet res = db.query("select from TestClass");
    assertEquals(res.next().getProperty("read"), "test");
    db.commit();
  }
}
