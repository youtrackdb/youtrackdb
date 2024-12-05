package com.orientechnologies.core.sql.parser;

import com.orientechnologies.core.command.OBasicCommandContext;
import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.config.OStorageConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.OMetadataUpdateListener;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.metadata.schema.OSchemaShared;
import com.orientechnologies.core.sql.executor.OExecutionPlan;
import com.orientechnologies.core.sql.executor.OInternalExecutionPlan;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is an LRU cache for already prepared SQL execution plans. It stores itself in the
 * storage as a resource. It also acts an an entry point for the SQL executor.
 */
public class OExecutionPlanCache implements OMetadataUpdateListener {

  Map<String, OInternalExecutionPlan> map;
  int mapSize;

  protected long lastInvalidation = -1;
  protected long lastGlobalTimeout = YTGlobalConfiguration.COMMAND_TIMEOUT.getValueAsLong();

  /**
   * @param size the size of the cache
   */
  public OExecutionPlanCache(int size) {
    this.mapSize = size;
    map =
        new LinkedHashMap<>(size) {
          protected boolean removeEldestEntry(
              final Map.Entry<String, OInternalExecutionPlan> eldest) {
            return super.size() > mapSize;
          }
        };
  }

  public static long getLastInvalidation(YTDatabaseSessionInternal db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }

    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
    synchronized (resource) {
      return resource.lastInvalidation;
    }
  }

  /**
   * @param statement an SQL statement
   * @return true if the corresponding executor is present in the cache
   */
  public boolean contains(String statement) {
    if (YTGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      return false;
    }
    synchronized (map) {
      return map.containsKey(statement);
    }
  }

  /**
   * returns an already prepared SQL execution plan, taking it from the cache if it exists or
   * creating a new one if it doesn't
   *
   * @param statement the SQL statement
   * @param ctx
   * @param db        the current DB instance
   * @return a statement executor from the cache
   */
  public static OExecutionPlan get(
      String statement, OCommandContext ctx, YTDatabaseSessionInternal db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }
    if (statement == null) {
      return null;
    }

    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
    OExecutionPlan result = resource.getInternal(statement, ctx, db);
    return result;
  }

  public static void put(String statement, OExecutionPlan plan, YTDatabaseSessionInternal db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }
    if (statement == null) {
      return;
    }

    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
    resource.putInternal(statement, plan, db);
  }

  public void putInternal(String statement, OExecutionPlan plan, YTDatabaseSessionInternal db) {
    if (statement == null) {
      return;
    }

    if (YTGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      return;
    }

    synchronized (map) {
      OInternalExecutionPlan internal = (OInternalExecutionPlan) plan;
      OBasicCommandContext ctx = new OBasicCommandContext();
      ctx.setDatabase(db);
      internal = internal.copy(ctx);
      // this copy is never used, so it has to be closed to free resources
      internal.close();
      map.put(statement, internal);
    }
  }

  /**
   * @param statement an SQL statement
   * @param ctx
   * @return the corresponding executor, taking it from the internal cache, if it exists
   */
  public OExecutionPlan getInternal(
      String statement, OCommandContext ctx, YTDatabaseSessionInternal db) {
    OInternalExecutionPlan result;

    long currentGlobalTimeout =
        db.getConfiguration().getValueAsLong(YTGlobalConfiguration.COMMAND_TIMEOUT);
    if (currentGlobalTimeout != this.lastGlobalTimeout) {
      invalidate();
    }
    this.lastGlobalTimeout = currentGlobalTimeout;

    if (statement == null) {
      return null;
    }
    if (YTGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      return null;
    }
    synchronized (map) {
      // LRU
      result = map.remove(statement);
      if (result != null) {
        map.put(statement, result);
        result = result.copy(ctx);
      }
    }

    return result;
  }

  public void invalidate() {
    if (YTGlobalConfiguration.STATEMENT_CACHE_SIZE.getValueAsInteger() == 0) {
      lastInvalidation = System.currentTimeMillis();
      return;
    }

    synchronized (this) {
      synchronized (map) {
        map.clear();
      }
      lastInvalidation = System.currentTimeMillis();
    }
  }

  @Override
  public void onSchemaUpdate(YTDatabaseSessionInternal session, String database,
      OSchemaShared schema) {
    invalidate();
  }

  @Override
  public void onIndexManagerUpdate(YTDatabaseSessionInternal session, String database,
      OIndexManagerAbstract indexManager) {
    invalidate();
  }

  @Override
  public void onFunctionLibraryUpdate(YTDatabaseSessionInternal session, String database) {
    invalidate();
  }

  @Override
  public void onSequenceLibraryUpdate(YTDatabaseSessionInternal session, String database) {
    invalidate();
  }

  @Override
  public void onStorageConfigurationUpdate(String database, OStorageConfiguration update) {
    invalidate();
  }

  public static OExecutionPlanCache instance(YTDatabaseSessionInternal db) {
    if (db == null) {
      throw new IllegalArgumentException("DB cannot be null");
    }

    OExecutionPlanCache resource = db.getSharedContext().getExecutionPlanCache();
    return resource;
  }
}
