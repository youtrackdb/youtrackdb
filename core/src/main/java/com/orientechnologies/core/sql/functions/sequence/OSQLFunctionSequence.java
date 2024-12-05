package com.orientechnologies.core.sql.functions.sequence;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.metadata.sequence.YTSequence;
import com.orientechnologies.core.sql.filter.OSQLFilterItem;
import com.orientechnologies.core.sql.functions.OSQLFunctionConfigurableAbstract;

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
