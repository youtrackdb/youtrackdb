package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * <p>Fetches temporary records (cluster id -1) from current transaction
 */
public class FetchTemporaryFromTxStep extends AbstractExecutionStep {

  private String className;

  private Object order;

  public FetchTemporaryFromTxStep(OCommandContext ctx, String className, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.className = className;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Iterator<Record> data;
    data = init(ctx);
    return OExecutionStream.iterator(data).map(this::setContext);
  }

  private YTResult setContext(YTResult result, OCommandContext context) {
    context.setVariable("$current", result);
    return result;
  }

  private Iterator<Record> init(OCommandContext ctx) {
    Iterable<? extends ORecordOperation> iterable =
        ctx.getDatabase().getTransaction().getRecordOperations();

    List<Record> records = new ArrayList<>();
    if (iterable != null) {
      for (ORecordOperation op : iterable) {
        Record record = op.record;
        if (matchesClass(record, className) && !hasCluster(record)) {
          records.add(record);
        }
      }
    }
    if (order == FetchFromClusterExecutionStep.ORDER_ASC) {
      records.sort(
          (o1, o2) -> {
            long p1 = o1.getIdentity().getClusterPosition();
            long p2 = o2.getIdentity().getClusterPosition();
            return Long.compare(p1, p2);
          });
    } else {
      records.sort(
          (o1, o2) -> {
            long p1 = o1.getIdentity().getClusterPosition();
            long p2 = o2.getIdentity().getClusterPosition();
            return Long.compare(p2, p1);
          });
    }
    return records.iterator();
  }

  private static boolean hasCluster(Record record) {
    YTRID rid = record.getIdentity();
    if (rid == null) {
      return false;
    }
    return rid.getClusterId() >= 0;
  }

  private static boolean matchesClass(Record record, String className) {
    Record doc = record.getRecord();
    if (!(doc instanceof EntityImpl)) {
      return false;
    }

    YTClass schema = ODocumentInternal.getImmutableSchemaClass(((EntityImpl) doc));
    if (schema == null) {
      return className == null;
    } else if (schema.getName().equals(className)) {
      return true;
    } else {
      return schema.isSubClassOf(className);
    }
  }

  public void setOrder(Object order) {
    this.order = order;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ FETCH NEW RECORDS FROM CURRENT TRANSACTION SCOPE (if any)");
    if (profilingEnabled) {
      result.append(" (").append(getCostFormatted()).append(")");
    }
    return result.toString();
  }

  @Override
  public YTResult serialize(YTDatabaseSessionInternal db) {
    YTResultInternal result = OExecutionStepInternal.basicSerialize(db, this);
    result.setProperty("className", className);
    return result;
  }

  @Override
  public void deserialize(YTResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      className = fromResult.getProperty("className");
    } catch (Exception e) {
      throw YTException.wrapException(new YTCommandExecutionException(""), e);
    }
  }

  @Override
  public boolean canBeCached() {
    return true;
  }

  @Override
  public OExecutionStep copy(OCommandContext ctx) {
    return new FetchTemporaryFromTxStep(ctx, this.className, profilingEnabled);
  }
}
