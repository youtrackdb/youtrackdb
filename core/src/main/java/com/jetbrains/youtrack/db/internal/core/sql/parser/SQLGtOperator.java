/* Generated By:JJTree: Do not edit this line. SQLGtOperator.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder.Operation;
import java.util.Map;

public class SQLGtOperator extends SimpleNode implements SQLBinaryCompareOperator {

  public SQLGtOperator(int id) {
    super(id);
  }

  public SQLGtOperator(YouTrackDBSql p, int id) {
    super(p, id);
  }

  @Override
  public boolean execute(Object iLeft, Object iRight) {
    if (iLeft == null || iRight == null) {
      return false;
    }
    if (iLeft.getClass() != iRight.getClass()
        && iLeft instanceof Number
        && iRight instanceof Number) {
      Number[] couple = PropertyType.castComparableNumber((Number) iLeft, (Number) iRight);
      iLeft = couple[0];
      iRight = couple[1];
    } else {
      try {
        iRight = PropertyType.convert(null, iRight, iLeft.getClass());
      } catch (RuntimeException e) {
        iRight = null;
        // Can't convert to the target value do nothing will return false
        LogManager.instance()
            .warn(this, "Issue converting value to target type, ignoring value", e);
      }
    }
    if (iRight == null) {
      return false;
    }
    if (iLeft instanceof Identifiable && !(iRight instanceof Identifiable)) {
      return false;
    }
    if (!(iLeft instanceof Comparable)) {
      return false;
    }
    return ((Comparable<Object>) iLeft).compareTo(iRight) > 0;
  }

  @Override
  public String toString() {
    return ">";
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append(">");
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(">");
  }

  @Override
  public boolean supportsBasicCalculation() {
    return true;
  }

  @Override
  public SQLGtOperator copy() {
    return this;
  }

  @Override
  public boolean isRangeOperator() {
    return true;
  }

  @Override
  public Operation getOperation() {
    return Operation.Gt;
  }

  @Override
  public boolean equals(Object obj) {
    return obj != null && obj.getClass().equals(this.getClass());
  }

  @Override
  public int hashCode() {
    return getClass().hashCode();
  }
}
/* JavaCC - OriginalChecksum=4b96739fc6e9ae496916d542db361376 (do not edit this line) */
