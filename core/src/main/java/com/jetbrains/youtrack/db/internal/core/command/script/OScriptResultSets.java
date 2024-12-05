package com.jetbrains.youtrack.db.internal.core.command.script;

import com.jetbrains.youtrack.db.internal.core.command.script.transformer.OScriptTransformer;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import java.util.Collections;

/**
 * Static Factories of YTScriptResultSet objects
 *
 * <p>Used in script results with conversion to YTResult for single iteration
 * on 27/01/17.
 */
public class OScriptResultSets {

  /**
   * Empty result set
   *
   * @return
   */
  public static YTScriptResultSet empty(YTDatabaseSessionInternal db) {
    return new YTScriptResultSet(db, Collections.EMPTY_LIST.iterator(), null);
  }

  /**
   * Result set with a single result;
   *
   * @return
   */
  public static YTScriptResultSet singleton(YTDatabaseSessionInternal db, Object entity,
      OScriptTransformer transformer) {
    return new YTScriptResultSet(db, Collections.singletonList(entity).iterator(), transformer);
  }
}
