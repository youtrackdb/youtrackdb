/* Generated By:JJTree: Do not edit this line. OHaRemoveServerStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

public class OHaRemoveServerStatement extends OStatement {

  public OIdentifier serverName;

  public OHaRemoveServerStatement(int id) {
    super(id);
  }

  public OHaRemoveServerStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("HA REMOVE SERVER ");
    serverName.toString(params, builder);
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("HA REMOVE SERVER ");
    serverName.toGenericStatement(builder);
  }

  @Override
  public OResultSet execute(
      ODatabaseDocumentInternal db,
      Object[] args,
      OCommandContext parentContext,
      boolean usePlanCache) {
    ODatabaseDocumentInternal internalDb = (ODatabaseDocumentInternal) db;
    boolean res = internalDb.removeHaServer(serverName.getStringValue());
    OResultInternal r = new OResultInternal();
    r.setProperty("result", res);
    OInternalResultSet rs = new OInternalResultSet();
    rs.add(r);
    return rs;
  }

  @Override
  public OResultSet execute(
      ODatabaseDocumentInternal db, Map args, OCommandContext parentContext, boolean usePlanCache) {
    ODatabaseDocumentInternal internalDb = (ODatabaseDocumentInternal) db;
    boolean res = internalDb.removeHaServer(serverName.getStringValue());
    OResultInternal r = new OResultInternal();
    r.setProperty("result", res);
    OInternalResultSet rs = new OInternalResultSet();
    rs.add(r);
    return rs;
  }
}
/* JavaCC - OriginalChecksum=9c136e8917527d69a67c88582d20ac8f (do not edit this line) */
