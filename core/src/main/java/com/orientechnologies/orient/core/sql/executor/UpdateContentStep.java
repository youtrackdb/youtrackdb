package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.YTTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OInputParameter;
import com.orientechnologies.orient.core.sql.parser.OJson;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class UpdateContentStep extends AbstractExecutionStep {

  private OJson json;
  private OInputParameter inputParameter;

  public UpdateContentStep(OJson json, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.json = json;
  }

  public UpdateContentStep(
      OInputParameter inputParameter, OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.inputParameter = inputParameter;
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws YTTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result instanceof OResultInternal) {
      var elem = result.toElement();
      assert elem != null;
      handleContent((YTEntityInternal) elem, ctx);
    }
    return result;
  }

  private void handleContent(YTEntityInternal record, OCommandContext ctx) {
    // REPLACE ALL THE CONTENT
    YTDocument fieldsToPreserve = null;

    YTClass clazz = record.getSchemaType().orElse(null);
    if (clazz != null && ((YTImmutableClass) clazz).isRestricted()) {
      fieldsToPreserve = new YTDocument();

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
            fieldsToPreserve = new YTDocument();
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyInternal(fieldName));
        }
      }
    } else if (recordClass != null && recordClass.isSubClassOf("E")) {
      for (String fieldName : record.getPropertyNamesInternal()) {
        if (fieldName.equals("in") || fieldName.equals("out")) {
          if (fieldsToPreserve == null) {
            fieldsToPreserve = new YTDocument();
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyInternal(fieldName));
        }
      }
    }
    YTDocument doc = record.getRecord();
    if (json != null) {
      doc.merge(json.toDocument(record, ctx), false, false);
    } else if (inputParameter != null) {
      Object val = inputParameter.getValue(ctx.getInputParameters());
      if (val instanceof YTEntity) {
        doc.merge(((YTEntity) val).getRecord(), false, false);
      } else if (val instanceof Map<?, ?> map) {
        var mapDoc = new YTDocument();
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
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
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
