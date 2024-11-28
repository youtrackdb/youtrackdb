package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import java.util.Collections;

/**
 * Static Factories of OScriptResultSet objects
 *
 * <p>Used in script results with conversion to OResult for single iteration
 * on 27/01/17.
 */
public class OScriptResultSets {

  /**
   * Empty result set
   *
   * @return
   */
  public static OScriptResultSet empty(ODatabaseSessionInternal db) {
    return new OScriptResultSet(db, Collections.EMPTY_LIST.iterator(), null);
  }

  /**
   * Result set with a single result;
   *
   * @return
   */
  public static OScriptResultSet singleton(ODatabaseSessionInternal db, Object entity,
      OScriptTransformer transformer) {
    return new OScriptResultSet(db, Collections.singletonList(entity).iterator(), transformer);
  }
}
