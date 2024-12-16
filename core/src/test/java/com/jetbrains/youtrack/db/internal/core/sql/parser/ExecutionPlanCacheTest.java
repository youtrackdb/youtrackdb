package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import org.junit.Assert;
import org.junit.Test;

public class ExecutionPlanCacheTest extends BaseMemoryInternalDatabase {

  @Test
  public void testCacheInvalidation1() throws InterruptedException {
    String testName = "testCacheInvalidation1";
    ExecutionPlanCache cache = ExecutionPlanCache.instance(db);
    String stm = "SELECT FROM OUser";

    /*
     * the cache has a mechanism that guarantees that if you are doing execution planning
     * and the cache is invalidated in the meantime, the newly generated execution plan
     * is not cached. This mechanism relies on a System.currentTimeMillis(), so it can happen
     * that the execution planning is done right after the cache invalidation, but still in THE SAME
     * millisecond, this Thread.sleep() guarantees that the new execution plan is generated
     * at least one ms after last invalidation, so it is cached.
     */
    Thread.sleep(2);

    // schema changes
    db.query(stm).close();
    cache = ExecutionPlanCache.instance(db);
    Assert.assertTrue(cache.contains(stm));

    SchemaClass clazz = db.getMetadata().getSchema().createClass(testName);
    Assert.assertFalse(cache.contains(stm));

    Thread.sleep(2);

    // schema changes 2
    db.query(stm).close();
    cache = ExecutionPlanCache.instance(db);
    Assert.assertTrue(cache.contains(stm));

    Property prop = clazz.createProperty(db, "name", PropertyType.STRING);
    Assert.assertFalse(cache.contains(stm));

    Thread.sleep(2);

    // index changes
    db.query(stm).close();
    cache = ExecutionPlanCache.instance(db);
    Assert.assertTrue(cache.contains(stm));

    prop.createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);
    Assert.assertFalse(cache.contains(stm));
  }
}
