package com.orientechnologies.orient.core.db.hook;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.hook.YTDocumentHookAbstract;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.UUID;
import org.junit.Test;

public class CheckHookCallCountTest extends DBTestBase {

  private final String CLASS_NAME = "Data";
  private final String FIELD_ID = "ID";
  private final String FIELD_STATUS = "STATUS";
  private final String STATUS = "processed";

  @Test
  public void testMultipleCallHook() {
    YTClass aClass = db.getMetadata().getSchema().createClass(CLASS_NAME);
    aClass.createProperty(db, FIELD_ID, YTType.STRING);
    aClass.createProperty(db, FIELD_STATUS, YTType.STRING);
    aClass.createIndex(db, "IDX", YTClass.INDEX_TYPE.NOTUNIQUE, FIELD_ID);
    TestHook hook = new TestHook();
    db.registerHook(hook);

    String id = UUID.randomUUID().toString();
    db.begin();
    YTEntityImpl first = new YTEntityImpl(CLASS_NAME);
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
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oClass = schema.createClass("TestInHook");
    oClass.createProperty(db, "a", YTType.INTEGER);
    oClass.createProperty(db, "b", YTType.INTEGER);
    oClass.createProperty(db, "c", YTType.INTEGER);

    db.begin();
    YTEntityImpl doc = new YTEntityImpl(oClass);
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
        new YTDocumentHookAbstract(db) {

          {
            setIncludeClasses("TestInHook");
          }

          @Override
          public void onRecordAfterCreate(YTEntityImpl iDocument) {
            onRecordAfterRead(iDocument);
          }

          @Override
          public void onRecordAfterRead(YTEntityImpl iDocument) {
            String script = "select sum(a, b) as value from " + iDocument.getIdentity();
            try (YTResultSet calculated = database.query(script)) {
              if (calculated.hasNext()) {
                iDocument.field("c", calculated.next().<Object>getProperty("value"));
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
    doc = new YTEntityImpl(oClass);
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

  public class TestHook extends YTDocumentHookAbstract {

    public int readCount;

    @Override
    public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
      return DISTRIBUTED_EXECUTION_MODE.BOTH;
    }

    @Override
    public void onRecordAfterRead(YTEntityImpl iDocument) {
      readCount++;
    }
  }
}
