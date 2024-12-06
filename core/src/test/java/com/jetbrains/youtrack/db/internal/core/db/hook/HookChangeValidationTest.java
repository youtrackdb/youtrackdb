package com.jetbrains.youtrack.db.internal.core.db.hook;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.ValidationException;
import com.jetbrains.youtrack.db.internal.core.hook.DocumentHookAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

public class HookChangeValidationTest extends DbTestBase {

  @Test
  public void testHookCreateChangeTx() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass classA = schema.createClass("TestClass");
    classA.createProperty(db, "property1", PropertyType.STRING).setNotNull(db, true);
    classA.createProperty(db, "property2", PropertyType.STRING).setReadonly(db, true);
    classA.createProperty(db, "property3", PropertyType.STRING).setMandatory(db, true);
    db.registerHook(
        new DocumentHookAbstract() {
          @Override
          public RESULT onRecordBeforeCreate(EntityImpl entity) {
            entity.removeField("property1");
            entity.removeField("property2");
            entity.removeField("property3");
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public RESULT onRecordBeforeUpdate(EntityImpl entity) {
            return RESULT.RECORD_NOT_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
          }
        });
    EntityImpl doc = new EntityImpl(classA);
    doc.field("property1", "value1-create");
    doc.field("property2", "value2-create");
    doc.field("property3", "value3-create");
    try {
      db.begin();
      doc.save();
      db.commit();
      Assert.fail("The document save should fail for validation exception");
    } catch (ValidationException ex) {

    }
  }

  @Test
  public void testHookUpdateChangeTx() {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass classA = schema.createClass("TestClass");
    classA.createProperty(db, "property1", PropertyType.STRING).setNotNull(db, true);
    classA.createProperty(db, "property2", PropertyType.STRING).setReadonly(db, true);
    classA.createProperty(db, "property3", PropertyType.STRING).setMandatory(db, true);
    db.registerHook(
        new DocumentHookAbstract() {
          @Override
          public RESULT onRecordBeforeCreate(EntityImpl entity) {
            return RESULT.RECORD_NOT_CHANGED;
          }

          @Override
          public RESULT onRecordBeforeUpdate(EntityImpl entity) {
            entity.removeField("property1");
            entity.removeField("property2");
            entity.removeField("property3");
            return RESULT.RECORD_CHANGED;
          }

          @Override
          public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
            return DISTRIBUTED_EXECUTION_MODE.SOURCE_NODE;
          }
        });

    db.begin();
    EntityImpl doc = new EntityImpl(classA);
    doc.field("property1", "value1-create");
    doc.field("property2", "value2-create");
    doc.field("property3", "value3-create");
    doc.save();
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    assertEquals("value1-create", doc.field("property1"));
    assertEquals("value2-create", doc.field("property2"));
    assertEquals("value3-create", doc.field("property3"));

    doc.field("property1", "value1-update");
    doc.field("property2", "value2-update");
    try {
      doc.save();
      db.commit();
      Assert.fail("The document save should fail for validation exception");
    } catch (ValidationException ex) {

    }
  }
}
