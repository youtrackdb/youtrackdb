package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.hook.YTRecordHook;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import org.junit.Test;

/**
 *
 */
public class HookSaveTest extends DBTestBase {

  @Test
  public void testCreatedLinkedInHook() {
    db.registerHook(
        new YTRecordHook() {
          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(TYPE iType, YTRecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTEntityImpl doc = (YTEntityImpl) iRecord;
            if (doc.containsField("test")) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTEntityImpl doc1 = new YTEntityImpl("test");
            doc1.field("test", "value");
            doc.field("testNewLinkedRecord", doc1);
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        });

    db.getMetadata().getSchema().createClass("test");
    db.begin();
    YTEntityImpl doc = db.save(new YTEntityImpl("test"));
    db.commit();

    YTEntityImpl newRef = db.bindToSession(doc).field("testNewLinkedRecord");
    assertNotNull(newRef);
    assertNotNull(newRef.getIdentity().isPersistent());
  }

  @Test
  public void testCreatedBackLinkedInHook() {
    db.registerHook(
        new YTRecordHook() {
          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(TYPE iType, YTRecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTEntityImpl doc = (YTEntityImpl) iRecord;
            if (doc.containsField("test")) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTEntityImpl doc1 = new YTEntityImpl("test");
            doc1.field("test", "value");
            doc.field("testNewLinkedRecord", doc1);
            doc1.field("backLink", doc);
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return null;
          }
        });

    db.getMetadata().getSchema().createClass("test");
    db.begin();
    YTEntityImpl doc = db.save(new YTEntityImpl("test"));
    db.commit();

    YTEntityImpl newRef = db.bindToSession(doc).field("testNewLinkedRecord");
    assertNotNull(newRef);
    assertNotNull(newRef.getIdentity().isPersistent());
  }
}
