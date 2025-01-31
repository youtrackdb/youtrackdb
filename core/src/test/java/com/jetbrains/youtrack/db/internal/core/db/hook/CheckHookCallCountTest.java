package com.jetbrains.youtrack.db.internal.core.db.hook;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.hook.DocumentHookAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.UUID;
import org.junit.Test;

public class CheckHookCallCountTest extends DbTestBase {

  private final String CLASS_NAME = "Data";
  private final String FIELD_ID = "ID";
  private final String FIELD_STATUS = "STATUS";
  private final String STATUS = "processed";

  @Test
  public void testMultipleCallHook() {
    var aClass = db.getMetadata().getSchema().createClass(CLASS_NAME);
    aClass.createProperty(db, FIELD_ID, PropertyType.STRING);
    aClass.createProperty(db, FIELD_STATUS, PropertyType.STRING);
    aClass.createIndex(db, "IDX", SchemaClass.INDEX_TYPE.NOTUNIQUE, FIELD_ID);
    var hook = new TestHook();
    db.registerHook(hook);

    var id = UUID.randomUUID().toString();
    db.begin();
    var first = (EntityImpl) db.newEntity(CLASS_NAME);
    first.field(FIELD_ID, id);
    first.field(FIELD_STATUS, STATUS);
    db.save(first);
    db.commit();

    db
        .query("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD_STATUS + " = '" + STATUS + "'")
        .stream()
        .count();
    //      assertEquals(hook.readCount, 1); //TODO
    hook.readCount = 0;
    db.query("SELECT FROM " + CLASS_NAME + " WHERE " + FIELD_ID + " = '" + id + "'").stream()
        .count();
    //      assertEquals(hook.readCount, 1); //TODO
  }

  @Test
  public void testInHook() throws Exception {
    Schema schema = db.getMetadata().getSchema();
    var oClass = schema.createClass("TestInHook");
    oClass.createProperty(db, "a", PropertyType.INTEGER);
    oClass.createProperty(db, "b", PropertyType.INTEGER);
    oClass.createProperty(db, "c", PropertyType.INTEGER);

    db.begin();
    var doc = (EntityImpl) db.newEntity(oClass);
    doc.field("a", 2);
    doc.field("b", 2);
    doc.save();
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    assertEquals(Integer.valueOf(2), doc.field("a"));
    assertEquals(Integer.valueOf(2), doc.field("b"));
    assertNull(doc.field("c"));
    db.rollback();

    db.registerHook(
        new DocumentHookAbstract(db) {

          {
            setIncludeClasses("TestInHook");
          }

          @Override
          public void onRecordAfterCreate(EntityImpl entity) {
            onRecordAfterRead(entity);
          }

          @Override
          public void onRecordAfterRead(EntityImpl entity) {
            var script = "select sum(a, b) as value from " + entity.getIdentity();
            try (var calculated = database.query(script)) {
              if (calculated.hasNext()) {
                entity.field("c", calculated.next().<Object>getProperty("value"));
              }
            }
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
          }
        });

    db.begin();
    doc = db.bindToSession(doc);
    assertEquals(Integer.valueOf(2), doc.field("a"));
    assertEquals(Integer.valueOf(2), doc.field("b"));
    assertEquals(Integer.valueOf(4), doc.field("c"));
    db.rollback();

    db.begin();
    doc = (EntityImpl) db.newEntity(oClass);
    doc.field("a", 3);
    doc.field("b", 3);
    doc.save();
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    assertEquals(Integer.valueOf(3), doc.field("a"));
    assertEquals(Integer.valueOf(3), doc.field("b"));
    assertEquals(Integer.valueOf(6), doc.field("c"));
    db.rollback();
  }

  public class TestHook extends DocumentHookAbstract {

    public int readCount;

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.BOTH;
    }

    @Override
    public void onRecordAfterRead(EntityImpl entity) {
      readCount++;
    }
  }
}
