package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.security.OSecurity;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.OElementInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OInputParameter;
import com.orientechnologies.orient.core.sql.parser.OJson;
import java.util.HashMap;
import java.util.Map;

/** Created by luigidellaquila on 09/08/16. */
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
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    assert prev != null;
    OExecutionStream upstream = prev.start(ctx);
    return upstream.map(this::mapResult);
  }

  private OResult mapResult(OResult result, OCommandContext ctx) {
    if (result instanceof OResultInternal) {
      var elem = result.toElement();
      assert elem != null;
      handleContent((OElementInternal) elem, ctx);
    }
    return result;
  }

  private void handleContent(OElementInternal record, OCommandContext ctx) {
    // REPLACE ALL THE CONTENT
    ODocument fieldsToPreserve = null;

    OClass clazz = record.getSchemaType().orElse(null);
    if (clazz != null && ((OImmutableClass) clazz).isRestricted()) {
      fieldsToPreserve = new ODocument();

      final OClass restricted =
          ((ODatabaseSessionInternal) ctx.getDatabase())
              .getMetadata()
              .getImmutableSchemaSnapshot()
              .getClass(OSecurity.RESTRICTED_CLASSNAME);
      for (OProperty prop : restricted.properties()) {
        fieldsToPreserve.field(prop.getName(), record.<Object>getProperty(prop.getName()));
      }
    }
    Map<String, Object> preDefaultValues = null;
    if (clazz != null) {
      for (OProperty prop : clazz.properties()) {
        if (prop.getDefaultValue() != null) {
          if (preDefaultValues == null) {
            preDefaultValues = new HashMap<>();
          }
          preDefaultValues.put(
              prop.getName(), record.<Object>getPropertyWithoutValidation(prop.getName()));
        }
      }
    }

    OClass recordClass =
        ODocumentInternal.getImmutableSchemaClass(
            (ODatabaseSessionInternal) ctx.getDatabase(), record.getRecord());
    if (recordClass != null && recordClass.isSubClassOf("V")) {
      for (String fieldName : record.getPropertyNamesWithoutFiltration()) {
        if (fieldName.startsWith("in_") || fieldName.startsWith("out_")) {
          if (fieldsToPreserve == null) {
            fieldsToPreserve = new ODocument();
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyWithoutValidation(fieldName));
        }
      }
    } else if (recordClass != null && recordClass.isSubClassOf("E")) {
      for (String fieldName : record.getPropertyNamesWithoutFiltration()) {
        if (fieldName.equals("in") || fieldName.equals("out")) {
          if (fieldsToPreserve == null) {
            fieldsToPreserve = new ODocument();
          }
          fieldsToPreserve.field(fieldName, record.<Object>getPropertyWithoutValidation(fieldName));
        }
      }
    }
    ODocument doc = record.getRecord();
    if (json != null) {
      doc.merge(json.toDocument(record, ctx), false, false);
    } else if (inputParameter != null) {
      Object val = inputParameter.getValue(ctx.getInputParameters());
      if (val instanceof OElement) {
        doc.merge((ODocument) ((OElement) val).getRecord(), false, false);
      } else if (val instanceof Map<?, ?> map) {
        //noinspection unchecked
        doc.merge(new ODocument().fromMap((Map<String, ?>) map), false, false);
      } else {
        throw new OCommandExecutionException("Invalid value for UPDATE CONTENT: " + val);
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
