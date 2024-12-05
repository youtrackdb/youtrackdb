package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.hook.YTRecordHook;
import com.orientechnologies.orient.core.metadata.security.OSecurityPolicy;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
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
          public RESULT onTrigger(TYPE iType, YTRecord iRecord) {
            if (iType == TYPE.AFTER_READ
                && !((YTDocument) iRecord)
                .getClassName()
                .equalsIgnoreCase(OSecurityPolicy.class.getSimpleName())) {
              ((YTDocument) iRecord).field("read", "test");
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
    db.save(new YTDocument("TestClass"));
    db.commit();

    db.begin();
    YTResultSet res = db.query("select from TestClass");
    assertEquals(res.next().getProperty("read"), "test");
    db.commit();
  }
}
