package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Security;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLInputParameter;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLJson;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class UpdateContentStep extends AbstractExecutionStep {

  private SQLJson json;
  private SQLInputParameter inputParameter;

  public UpdateContentStep(SQLJson json, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;
  }

  public UpdateContentStep(
      SQLInputParameter inputParameter, CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.inputParameter = inputParameter;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    assert prev != null;
    var upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private Result mapResult(Result result, CommandContext ctx) {
    if (result instanceof ResultInternal) {
      var entity = result.asEntity();
      assert entity != null;
      handleContent((EntityInternal) entity, ctx);
    }

    return result;
  }

  private void handleContent(EntityInternal record, CommandContext ctx) {
    // REPLACE ALL THE CONTENT
    EntityImpl fieldsToPreserve = null;

    var session = ctx.getDatabaseSession();
    var clazz = record.getImmutableSchemaClass(session);
    if (clazz != null && clazz.isRestricted()) {
      fieldsToPreserve = new EntityImpl(session);

      final var restricted =
          session
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClass(Security.RESTRICTED_CLASSNAME);
      for (var prop : restricted.properties(session)) {
        fieldsToPreserve.field(prop.getName(session),
            record.<Object>getProperty(prop.getName(session)));
      }
    }
    Map<String, Object> preDefaultValues = null;
    if (clazz != null) {
      for (var prop : clazz.properties(session)) {
        if (prop.getDefaultValue(session) != null) {
          if (preDefaultValues == null) {
            preDefaultValues = new HashMap<>();
          }
          preDefaultValues.put(prop.getName(session),
              record.getPropertyInternal(prop.getName(session)));
        }
      }
    }

    SchemaImmutableClass result = null;
    final EntityImpl entity1 = record.getRecord(session);
    if (entity1 != null) {
      result = entity1.getImmutableSchemaClass(session);
    }
    SchemaClass recordClass =
        result;
    if (recordClass != null && recordClass.isSubClassOf(session, "V")) {
      for (var fieldName : record.getPropertyNamesInternal()) {
        if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
          if (fieldsToPreserve == null) {
            fieldsToPreserve = new EntityImpl(session);
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyInternal(fieldName));
        }
      }
    } else if (recordClass != null && recordClass.isSubClassOf(session, "E")) {
      for (var fieldName : record.getPropertyNamesInternal()) {
        if (fieldName.equals("in") || fieldName.equals("out")) {
          if (fieldsToPreserve == null) {
            fieldsToPreserve = new EntityImpl(session);
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyInternal(fieldName));
        }
      }
    }
    EntityImpl entity = record.getRecord(session);
    if (json != null) {
      entity.merge(json.toEntity(record, ctx), false, false);
    } else if (inputParameter != null) {
      var val = inputParameter.getValue(ctx.getInputParameters());
      if (val instanceof Entity) {
        entity.merge(((Entity) val).getRecord(session), false, false);
      } else if (val instanceof Map<?, ?> map) {
        var mapDoc = new EntityImpl(session);
        //noinspection unchecked
        mapDoc.updateFromMap((Map<String, ?>) map);
        entity.merge(mapDoc, false, false);
      } else {
        throw new CommandExecutionException(session, "Invalid value for UPDATE CONTENT: " + val);
      }
    }
    if (fieldsToPreserve != null) {
      entity.merge(fieldsToPreserve, true, false);
    }
    if (preDefaultValues != null) {
      for (var val : preDefaultValues.entrySet()) {
        if (!entity.containsField(val.getKey())) {
          entity.setProperty(val.getKey(), val.getValue());
        }
      }
    }
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var spaces = ExecutionStepInternal.getIndent(depth, indent);
    var result = new StringBuilder();
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
