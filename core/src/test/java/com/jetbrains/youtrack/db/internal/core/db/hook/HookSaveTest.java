package com.jetbrains.youtrack.db.internal.core.db.hook;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RecordHook;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

/**
 *
 */
public class HookSaveTest extends DbTestBase {

  @Test
  public void testCreatedLinkedInHook() {
    session.registerHook(
        new RecordHook() {
          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(DatabaseSession session, TYPE iType, DBRecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) {
              return RESULT.RECORD_NOT_CHANGED;
            }
            if (iRecord instanceof Entity entity) {
              var cls = entity.getSchemaClass();
              if (cls != null && cls.getName(session).equals("test")) {
                var newEntity = session.newEntity("another");
                entity.setProperty("testNewLinkedRecord", newEntity);
                return RESULT.RECORD_CHANGED;
              }
            }

            return RESULT.RECORD_NOT_CHANGED;
          }

        });

    session.getMetadata().getSchema().createClass("test");
    session.getMetadata().getSchema().createClass("another");

    session.begin();
    var entity = session.newEntity("test");
    session.commit();

    var newRef = session.bindToSession(entity).getEntityProperty("testNewLinkedRecord");
    assertNotNull(newRef);
    assertTrue(newRef.getIdentity().isPersistent());
  }

  @Test
  public void testCreatedBackLinkedInHook() {
    session.registerHook(
        new RecordHook() {
          @Override
          public void onUnregister() {
          }

          @Override
          public RESULT onTrigger(DatabaseSession session, TYPE iType, DBRecord iRecord) {
            if (iType != TYPE.BEFORE_CREATE) {
              return RESULT.RECORD_NOT_CHANGED;
            }

            if (iRecord instanceof Entity entity) {
              var cls = entity.getSchemaClass();
              if (cls != null && cls.getName(session).equals("test")) {
                var newEntity = HookSaveTest.this.session.newEntity("another");

                entity.setProperty("testNewLinkedRecord", newEntity);
                newEntity.setProperty("backLink", entity);

                return RESULT.RECORD_CHANGED;
              }
            }

            return RESULT.RECORD_NOT_CHANGED;
          }
        });

    session.getMetadata().getSchema().createClass("test");
    session.getMetadata().getSchema().createClass("another");

    session.begin();
    EntityImpl doc = session.save(session.newEntity("test"));
    session.commit();

    EntityImpl newRef = session.bindToSession(doc).field("testNewLinkedRecord");
    assertNotNull(newRef);
    assertTrue(newRef.getIdentity().isPersistent());
  }
}
