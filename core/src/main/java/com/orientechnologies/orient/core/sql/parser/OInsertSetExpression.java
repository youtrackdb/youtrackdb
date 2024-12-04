package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import java.util.Map;

/**
 *
 */
public class OInsertSetExpression {

  protected OIdentifier left;
  protected OExpression right;

  public void toString(Map<Object, Object> params, StringBuilder builder) {
    left.toString(params, builder);
    builder.append(" = ");
    right.toString(params, builder);
  }

  public OInsertSetExpression copy() {
    OInsertSetExpression result = new OInsertSetExpression();
    result.left = left == null ? null : left.copy();
    result.right = right == null ? null : right.copy();
    return result;
  }

  public OIdentifier getLeft() {
    return left;
  }

  public OExpression getRight() {
    return right;
  }

  public boolean isCacheable(YTDatabaseSessionInternal session) {
    return right.isCacheable(session);
  }

  public void toGenericStatement(StringBuilder builder) {
    left.toGenericStatement(builder);
    builder.append(" = ");
    right.toGenericStatement(builder);
  }
}
