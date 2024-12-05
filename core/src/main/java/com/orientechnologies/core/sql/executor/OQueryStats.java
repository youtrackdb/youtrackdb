package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.index.OIndex;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class contains statistics about graph structure and query execution.
 *
 * <p>To obtain a copy of this object, use
 */
public class OQueryStats {

  public Map<String, Long> stats = new ConcurrentHashMap<>();

  public static OQueryStats get(YTDatabaseSessionInternal db) {
    return db.getSharedContext().getQueryStats();
  }

  public long getIndexStats(
      String indexName,
      int params,
      boolean range,
      boolean additionalRange,
      YTDatabaseSession database) {
    String key =
        generateKey(
            "INDEX",
            indexName,
            String.valueOf(params),
            String.valueOf(range),
            String.valueOf(additionalRange));
    Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    if (database != null && database instanceof YTDatabaseSessionInternal db) {
      OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, indexName);
      if (idx != null
          && idx.isUnique()
          && (idx.getDefinition().getFields().size() == params)
          && !range) {
        return 1;
      }
    }
    return -1;
  }

  public void pushIndexStats(
      String indexName, int params, boolean range, boolean additionalRange, Long value) {
    String key =
        generateKey(
            "INDEX",
            indexName,
            String.valueOf(params),
            String.valueOf(range),
            String.valueOf(additionalRange));
    pushValue(key, value);
  }

  public long getAverageOutEdgeSpan(String vertexClass, String edgeClass) {
    String key = generateKey(vertexClass, "-", edgeClass, "->");
    Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public long getAverageInEdgeSpan(String vertexClass, String edgeClass) {
    String key = generateKey(vertexClass, "<-", edgeClass, "-");
    Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public long getAverageBothEdgeSpan(String vertexClass, String edgeClass) {
    String key = generateKey(vertexClass, "-", edgeClass, "-");
    Long val = stats.get(key);
    if (val != null) {
      return val;
    }
    return -1;
  }

  public void pushAverageOutEdgeSpan(String vertexClass, String edgeClass, Long value) {
    String key = generateKey(vertexClass, "-", edgeClass, "->");
    pushValue(key, value);
  }

  public void pushAverageInEdgeSpan(String vertexClass, String edgeClass, Long value) {
    String key = generateKey(vertexClass, "<-", edgeClass, "-");
    pushValue(key, value);
  }

  public void pushAverageBothEdgeSpan(String vertexClass, String edgeClass, Long value) {
    String key = generateKey(vertexClass, "-", edgeClass, "-");
    pushValue(key, value);
  }

  private void pushValue(String key, Long value) {
    if (value == null) {
      return;
    }
    Long val = stats.get(key);

    if (val == null) {
      val = value;
    } else {
      // refine this ;-)
      val = ((Double) ((val * .9) + (value * .1))).longValue();
      if (value > 0 && val == 0) {
        val = 1L;
      }
    }
    stats.put(key, val);
  }

  protected String generateKey(String... keys) {
    StringBuilder result = new StringBuilder();
    for (String s : keys) {
      result.append(".->");
      result.append(s);
    }
    return result.toString();
  }
}
