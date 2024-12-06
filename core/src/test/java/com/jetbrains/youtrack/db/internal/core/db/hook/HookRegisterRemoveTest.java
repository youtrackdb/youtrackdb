package com.jetbrains.youtrack.db.internal.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.hook.RecordHook;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class HookRegisterRemoveTest extends DbTestBase {

  @Test
  public void addAndRemoveHookTest() {
    final AtomicInteger integer = new AtomicInteger(0);
    RecordHook iHookImpl =
        new RecordHook() {

          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(TYPE iType, Record iRecord) {
            integer.incrementAndGet();
            return null;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        };
    db.registerHook(iHookImpl);

    db.begin();
    db.save(new EntityImpl().field("test", "test"),
        db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();
    assertEquals(3, integer.get());
    db.unregisterHook(iHookImpl);

    db.begin();
    db.save(new EntityImpl(), db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    assertEquals(3, integer.get());
  }
}
