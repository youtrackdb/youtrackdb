package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.common.concur.YTTimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurity;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.ODocumentInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OInputParameter;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OJson;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class UpdateContentStep extends AbstractExecutionStep {

  private OJson json;
  private OInputParameter inputParameter;

  public UpdateContentStep(OJson json, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;
  }

  public UpdateContentStep(
      OInputParameter inputParameter, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.inputParameter = inputParameter;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    ExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private YTResult mapResult(YTResult result, CommandContext ctx) {
    if (result instanceof YTResultInternal) {
      var elem = result.toEntity();
      assert elem != null;
      handleContent((EntityInternal) elem, ctx);
    }
    return result;
  }

  private void handleContent(EntityInternal record, CommandContext ctx) {
    // REPLACE ALL THE CONTENT
    EntityImpl fieldsToPreserve = null;

    YTClass clazz = record.getSchemaType().orElse(null);
    if (clazz != null && ((YTImmutableClass) clazz).isRestricted()) {
      fieldsToPreserve = new EntityImpl();

      final YTClass restricted =
          ctx.getDatabase()
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClass(OSecurity.RESTRICTED_CLASSNAME);
      for (YTProperty prop : restricted.properties(ctx.getDatabase())) {
        fieldsToPreserve.field(prop.getName(), record.<Object>getProperty(prop.getName()));
      }
    }
    Map<String, Object> preDefaultValues = null;
    if (clazz != null) {
      for (YTProperty prop : clazz.properties(ctx.getDatabase())) {
        if (prop.getDefaultValue() != null) {
          if (preDefaultValues == null) {
            preDefaultValues = new HashMap<>();
          }
          preDefaultValues.put(prop.getName(), record.getPropertyInternal(prop.getName()));
        }
      }
    }

    YTClass recordClass =
        ODocumentInternal.getImmutableSchemaClass(ctx.getDatabase(), record.getRecord());
    if (recordClass != null && recordClass.isSubClassOf("V")) {
      for (String fieldName : record.getPropertyNamesInternal()) {
        if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
          if (fieldsToPreserve == null) {
            fieldsToPreserve = new EntityImpl();
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyInternal(fieldName));
        }
      }
    } else if (recordClass != null && recordClass.isSubClassOf("E")) {
      for (String fieldName : record.getPropertyNamesInternal()) {
        if (fieldName.equals("in") || fieldName.equals("out")) {
          if (fieldsToPreserve == null) {
            fieldsToPreserve = new EntityImpl();
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyInternal(fieldName));
        }
      }
    }
    EntityImpl doc = record.getRecord();
    if (json != null) {
      doc.merge(json.toDocument(record, ctx), false, false);
    } else if (inputParameter != null) {
      Object val = inputParameter.getValue(ctx.getInputParameters());
      if (val instanceof Entity) {
        doc.merge(((Entity) val).getRecord(), false, false);
      } else if (val instanceof Map<?, ?> map) {
        var mapDoc = new EntityImpl();
        //noinspection unchecked
        mapDoc.fromMap((Map<String, ?>) map);
        doc.merge(mapDoc, false, false);
      } else {
        throw new YTCommandExecutionException("Invalid value for UPDATE CONTENT: " + val);
      }
    }
    if (fieldsToPreserve != null) {
      doc.merge(fieldsToPreserve, true, false);
    }
    if (preDefaultValues != null) {
      for (Map.Entry<String, Object> val : preDefaultValues.entrySet()) {
        if (!doc.containsField(val.getKey())) {
          doc.setProperty(val.getKey(), val.getValue());
        }
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ UPDATE CONTENT\n");
    result.append(spaces);
    result.append("  ");
    if (json != null) {
      result.append(json);
    } else {
      result.append(inputParameter);
    }
    return result.toString();
  }
}
