package com.jetbrains.youtrack.db.internal.core.command.script;

import com.jetbrains.youtrack.db.internal.core.command.script.transformer.OScriptTransformer;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.YTIteratorResultSet;
import java.util.Iterator;

/**
 * Wrapper of YTIteratorResultSet Used in script results with conversion to YTResult for single
 * iteration
 */
public class YTScriptResultSet extends YTIteratorResultSet {

  protected OScriptTransformer transformer;

  public YTScriptResultSet(YTDatabaseSessionInternal db, Iterator iter,
      OScriptTransformer transformer) {
    super(db, iter);
    this.transformer = transformer;
  }

  @Override
  public YTResult next() {

    Object next = iterator.next();
    return transformer.toResult(db, next);
  }
}
