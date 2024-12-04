package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OAndBlock;
import com.orientechnologies.orient.core.sql.parser.OBooleanExpression;
import com.orientechnologies.orient.core.sql.parser.OCluster;
import com.orientechnologies.orient.core.sql.parser.OFromClause;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.List;

/**
 *
 */
public class UpsertStep extends AbstractExecutionStep {

  private final OFromClause commandTarget;
  private final OWhereClause initialFilter;

  public UpsertStep(
      OFromClause target, OWhereClause where, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.commandTarget = target;
    this.initialFilter = where;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    var prev = this.prev;
    assert prev != null;

    OExecutionStream upstream = prev.start(ctx);
    if (upstream.hasNext(ctx)) {
      return upstream;
    }

    return OExecutionStream.singleton(createNewRecord(ctx, commandTarget, initialFilter));
  }

  private OResult createNewRecord(
      OCommandContext ctx, OFromClause commandTarget, OWhereClause initialFilter) {
    YTDocument doc;
    if (commandTarget.getItem().getIdentifier() != null) {
      doc = new YTDocument(commandTarget.getItem().getIdentifier().getStringValue());
    } else if (commandTarget.getItem().getCluster() != null) {
      OCluster cluster = commandTarget.getItem().getCluster();
      Integer clusterId = cluster.getClusterNumber();
      if (clusterId == null) {
        clusterId = ctx.getDatabase().getClusterIdByName(cluster.getClusterName());
      }
      YTClass clazz =
          ctx.getDatabase()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClassByClusterId(clusterId);
      doc = new YTDocument(clazz);
    } else {
      throw new YTCommandExecutionException(
          "Cannot execute UPSERT on target '" + commandTarget + "'");
    }

    OUpdatableResult result = new OUpdatableResult(ctx.getDatabase(), doc);
    if (initialFilter != null) {
      setContent(result, initialFilter);
    }
    return result;
  }

  private void setContent(OResultInternal doc, OWhereClause initialFilter) {
    List<OAndBlock> flattened = initialFilter.flatten();
    if (flattened.isEmpty()) {
      return;
    }
    if (flattened.size() > 1) {
      throw new YTCommandExecutionException("Cannot UPSERT on OR conditions");
    }
    OAndBlock andCond = flattened.get(0);
    for (OBooleanExpression condition : andCond.getSubBlocks()) {
      condition.transformToUpdateItem().ifPresent(x -> x.applyUpdate(doc, ctx));
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces
        + "+ INSERT (upsert, if needed)\n"
        + spaces
        + "  target: "
        + commandTarget
        + "\n"
        + spaces
        + "  content: "
        + initialFilter;
  }
}
