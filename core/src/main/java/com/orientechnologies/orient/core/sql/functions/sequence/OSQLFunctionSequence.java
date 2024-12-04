package com.orientechnologies.orient.core.sql.functions.sequence;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.sequence.YTSequence;
import com.orientechnologies.orient.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.orient.core.sql.functions.OSQLFunctionConfigurableAbstract;

/**
 * Returns a sequence by name.
 */
public class OSQLFunctionSequence extends OSQLFunctionConfigurableAbstract {

  public static final String NAME = "sequence";

  public OSQLFunctionSequence() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {
    final String seqName;
    if (configuredParameters != null
        && configuredParameters.length > 0
        && configuredParameters[0] instanceof OSQLFilterItem) // old stuff
    {
      seqName =
          (String)
              ((OSQLFilterItem) configuredParameters[0])
                  .getValue(iCurrentRecord, iCurrentResult, iContext);
    } else {
      seqName = "" + iParams[0];
    }

    YTSequence result =
        ODatabaseRecordThreadLocal.instance()
            .get()
            .getMetadata()
            .getSequenceLibrary()
            .getSequence(seqName);
    if (result == null) {
      throw new YTCommandExecutionException("Sequence not found: " + seqName);
    }
    return result;
  }

  @Override
  public Object getResult() {
    return null;
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return "sequence(<name>)";
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }
}
