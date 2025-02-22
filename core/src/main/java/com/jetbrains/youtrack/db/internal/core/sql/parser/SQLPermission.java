/* Generated By:JJTree: Do not edit this line. SQLPermission.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.jetbrains.youtrack.db.internal.core.sql.parser;

import java.util.Map;
import java.util.Objects;

public class SQLPermission extends SimpleNode {

  protected String permission;

  public SQLPermission(int id) {
    super(id);
  }

  public SQLPermission(YouTrackDBSql p, int id) {
    super(p, id);
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append(permission);
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append(PARAMETER_PLACEHOLDER);
  }

  @Override
  public SQLPermission copy() {
    SQLPermission result = new SQLPermission(-1);
    result.permission = permission;
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SQLPermission that = (SQLPermission) o;

    return Objects.equals(permission, that.permission);
  }

  @Override
  public int hashCode() {
    return permission != null ? permission.hashCode() : 0;
  }
}
/* JavaCC - OriginalChecksum=576b31633bf93fdbc597f7448fc3c3b3 (do not edit this line) */
