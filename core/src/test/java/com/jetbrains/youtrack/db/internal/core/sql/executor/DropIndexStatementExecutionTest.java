package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DropIndexStatementExecutionTest extends BaseMemoryInternalDatabase {

  @Test
  public void testPlain() {
    Index index =
        db.getMetadata()
            .getSchema()
            .createClass("testPlain")
            .createProperty(db, "bar", PropertyType.STRING)
            .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);
    String indexName = index.getName();

    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNotNull((db.getMetadata().getIndexManagerInternal()).getIndex(db, indexName));

    ResultSet result = db.command("drop index " + indexName);
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertEquals("drop index", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, indexName));
  }

  @Test
  public void testAll() {
    Index index =
        db.getMetadata()
            .getSchema()
            .createClass("testAll")
            .createProperty(db, "baz", PropertyType.STRING)
            .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);
    String indexName = index.getName();

    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNotNull(db.getMetadata().getIndexManagerInternal().getIndex(db, indexName));

    ResultSet result = db.command("drop index *");
    Assert.assertTrue(result.hasNext());
    Result next = result.next();
    Assert.assertEquals("drop index", next.getProperty("operation"));
    result.close();
    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, indexName));
    Assert.assertTrue(db.getMetadata().getIndexManagerInternal().getIndexes(db).isEmpty());
  }

  @Test
  public void testWrongName() {

    String indexName = "nonexistingindex";
    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, indexName));

    try {
      ResultSet result = db.command("drop index " + indexName);
      Assert.fail();
    } catch (CommandExecutionException ex) {
    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testIfExists() {

    String indexName = "nonexistingindex";
    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, indexName));

    try {
      ResultSet result = db.command("drop index " + indexName + " if exists");
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
