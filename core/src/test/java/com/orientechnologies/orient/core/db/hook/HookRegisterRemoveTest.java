package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;

public class HookRegisterRemoveTest extends DBTestBase {

  @Test
  public void addAndRemoveHookTest() {
    final AtomicInteger integer = new AtomicInteger(0);
    ORecordHook iHookImpl =
        new ORecordHook() {

          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(TYPE iType, YTRecord iRecord) {
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
    db.save(new YTDocument().field("test", "test"),
        db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();
    assertEquals(3, integer.get());
    db.unregisterHook(iHookImpl);

    db.begin();
    db.save(new YTDocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    assertEquals(3, integer.get());
  }
}
