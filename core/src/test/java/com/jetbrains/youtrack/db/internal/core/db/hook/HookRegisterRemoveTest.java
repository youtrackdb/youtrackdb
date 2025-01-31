package com.jetbrains.youtrack.db.internal.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.RecordHook;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class HookRegisterRemoveTest extends DbTestBase {

  @Test
  public void addAndRemoveHookTest() {
    final var integer = new AtomicInteger(0);
    var iHookImpl =
        new RecordHook() {

          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(DatabaseSession db, TYPE iType, DBRecord iRecord) {
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
    db.save(((EntityImpl) db.newEntity()).field("test", "test"));
    db.commit();
    assertEquals(3, integer.get());
    db.unregisterHook(iHookImpl);

    db.begin();
    db.save(db.newEntity());
    db.commit();

    assertEquals(3, integer.get());
  }
}
