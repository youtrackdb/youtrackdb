package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by luigidellaquila on 12/01/17.
 *
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
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    Iterator<ORecord> data;
    data = init(ctx);
    return OExecutionStream.iterator(data).map(this::setContext);
  }

  private OResult setContext(OResult result, OCommandContext context) {
    context.setVariable("$current", result);
    return result;
  }

  private Iterator<ORecord> init(OCommandContext ctx) {
    Iterable<? extends ORecordOperation> iterable =
        ctx.getDatabase().getTransaction().getRecordOperations();

    List<ORecord> records = new ArrayList<>();
    if (iterable != null) {
      for (ORecordOperation op : iterable) {
        ORecord record = op.getRecord();
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

  private static boolean hasCluster(ORecord record) {
    ORID rid = record.getIdentity();
    if (rid == null) {
      return false;
    }
    return rid.getClusterId() >= 0;
  }

  private static boolean matchesClass(ORecord record, String className) {
    ORecord doc = record.getRecord();
    if (!(doc instanceof ODocument)) {
      return false;
    }

    OClass schema = ODocumentInternal.getImmutableSchemaClass(((ODocument) doc));
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
  public OResult serialize() {
    OResultInternal result = OExecutionStepInternal.basicSerialize(this);
    result.setProperty("className", className);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      OExecutionStepInternal.basicDeserialize(fromResult, this);
      className = fromResult.getProperty("className");
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
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
