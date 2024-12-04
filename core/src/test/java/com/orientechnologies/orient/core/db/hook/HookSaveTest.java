package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertNotNull;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.hook.ORecordHook;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import org.junit.Test;

/**
 *
 */
public class HookSaveTest extends DBTestBase {

  @Test
  public void testCreatedLinkedInHook() {
    db.registerHook(
        new ORecordHook() {
          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(TYPE iType, YTRecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTDocument doc = (YTDocument) iRecord;
            if (doc.containsField("test")) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTDocument doc1 = new YTDocument("test");
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
    YTDocument doc = db.save(new YTDocument("test"));
    db.commit();

    YTDocument newRef = db.bindToSession(doc).field("testNewLinkedRecord");
    assertNotNull(newRef);
    assertNotNull(newRef.getIdentity().isPersistent());
  }

  @Test
  public void testCreatedBackLinkedInHook() {
    db.registerHook(
        new ORecordHook() {
          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(TYPE iType, YTRecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTDocument doc = (YTDocument) iRecord;
            if (doc.containsField("test")) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            YTDocument doc1 = new YTDocument("test");
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
    YTDocument doc = db.save(new YTDocument("test"));
    db.commit();

    YTDocument newRef = db.bindToSession(doc).field("testNewLinkedRecord");
    assertNotNull(newRef);
    assertNotNull(newRef.getIdentity().isPersistent());
  }
}
