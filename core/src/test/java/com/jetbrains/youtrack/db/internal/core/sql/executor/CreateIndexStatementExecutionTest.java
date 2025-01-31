package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CreateIndexStatementExecutionTest extends BaseMemoryInternalDatabase {

  @Test
  public void testPlain() {
    var className = "testPlain";
    var clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty(db, "name", PropertyType.STRING);

    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name"));
    var result =
        db.command("create index " + className + ".name on " + className + " (name) notunique");
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    result.close();
    var idx = db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name");
    Assert.assertNotNull(idx);
    Assert.assertFalse(idx.isUnique());
  }

  @Test
  public void testIfNotExists() {
    var className = "testIfNotExists";
    var clazz = db.getMetadata().getSchema().createClass(className);
    clazz.createProperty(db, "name", PropertyType.STRING);

    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name"));
    var result =
        db.command(
            "create index "
                + className
                + ".name IF NOT EXISTS on "
                + className
                + " (name) notunique");
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertFalse(result.hasNext());
    Assert.assertNotNull(next);
    result.close();
    var idx = db.getMetadata().getIndexManagerInternal().getIndex(db, className + ".name");
    Assert.assertNotNull(idx);
    Assert.assertFalse(idx.isUnique());

    result =
        db.command(
            "create index "
                + className
                + ".name IF NOT EXISTS on "
                + className
                + " (name) notunique");
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
