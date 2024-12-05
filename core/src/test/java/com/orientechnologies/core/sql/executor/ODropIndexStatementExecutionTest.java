package com.orientechnologies.core.sql.executor;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ODropIndexStatementExecutionTest extends BaseMemoryInternalDatabase {

  @Test
  public void testPlain() {
    OIndex index =
        db.getMetadata()
            .getSchema()
            .createClass("testPlain")
            .createProperty(db, "bar", YTType.STRING)
            .createIndex(db, YTClass.INDEX_TYPE.NOTUNIQUE);
    String indexName = index.getName();

    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNotNull((db.getMetadata().getIndexManagerInternal()).getIndex(db, indexName));

    YTResultSet result = db.command("drop index " + indexName);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertEquals("drop index", next.getProperty("operation"));
    Assert.assertFalse(result.hasNext());
    result.close();

    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNull(db.getMetadata().getIndexManagerInternal().getIndex(db, indexName));
  }

  @Test
  public void testAll() {
    OIndex index =
        db.getMetadata()
            .getSchema()
            .createClass("testAll")
            .createProperty(db, "baz", YTType.STRING)
            .createIndex(db, YTClass.INDEX_TYPE.NOTUNIQUE);
    String indexName = index.getName();

    db.getMetadata().getIndexManagerInternal().reload(db);
    Assert.assertNotNull(db.getMetadata().getIndexManagerInternal().getIndex(db, indexName));

    YTResultSet result = db.command("drop index *");
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
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
      YTResultSet result = db.command("drop index " + indexName);
      Assert.fail();
    } catch (YTCommandExecutionException ex) {
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
      YTResultSet result = db.command("drop index " + indexName + " if exists");
    } catch (Exception e) {
      Assert.fail();
    }
  }
}
