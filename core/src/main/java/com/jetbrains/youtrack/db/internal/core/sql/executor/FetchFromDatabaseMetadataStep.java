package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ProduceExecutionStream;

/**
 * Returns an YTResult containing metadata regarding the database
 */
public class FetchFromDatabaseMetadataStep extends AbstractExecutionStep {

  public FetchFromDatabaseMetadataStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new ProduceExecutionStream(FetchFromDatabaseMetadataStep::produce).limit(1);
  }

  private static YTResult produce(CommandContext ctx) {
    var db = ctx.getDatabase();
    YTResultInternal result = new YTResultInternal(db);


    result.setProperty("name", db.getName());
    result.setProperty("user", db.getUser() == null ? null : db.getUser().getName(db));
    result.setProperty("type", String.valueOf(db.get(ATTRIBUTES.TYPE)));
    result.setProperty("status", String.valueOf(db.get(ATTRIBUTES.STATUS)));
    result.setProperty(
        "defaultClusterId", String.valueOf(db.get(ATTRIBUTES.DEFAULTCLUSTERID)));
    result.setProperty(
        "dateFormat", String.valueOf(db.get(ATTRIBUTES.DATEFORMAT)));
    result.setProperty(
        "dateTimeFormat", String.valueOf(db.get(ATTRIBUTES.DATETIMEFORMAT)));
    result.setProperty("timezone", String.valueOf(db.get(ATTRIBUTES.TIMEZONE)));
    result.setProperty(
        "localeCountry", String.valueOf(db.get(ATTRIBUTES.LOCALECOUNTRY)));
    result.setProperty(
        "localeLanguage", String.valueOf(db.get(ATTRIBUTES.LOCALELANGUAGE)));
    result.setProperty("charset", String.valueOf(db.get(ATTRIBUTES.CHARSET)));
    result.setProperty(
        "clusterSelection", String.valueOf(db.get(ATTRIBUTES.CLUSTERSELECTION)));
    result.setProperty(
        "minimumClusters", String.valueOf(db.get(ATTRIBUTES.MINIMUMCLUSTERS)));
    result.setProperty(
        "conflictStrategy", String.valueOf(db.get(ATTRIBUTES.CONFLICTSTRATEGY)));
    result.setProperty(
        "validation", String.valueOf(db.get(ATTRIBUTES.VALIDATION)));

    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ FETCH DATABASE METADATA";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
