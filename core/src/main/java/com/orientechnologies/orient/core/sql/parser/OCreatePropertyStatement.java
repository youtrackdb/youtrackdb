/* Generated By:JJTree: Do not edit this line. OCreatePropertyStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassEmbedded;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class OCreatePropertyStatement extends ODDLStatement {

  public OIdentifier className;
  public OIdentifier propertyName;
  boolean ifNotExists = false;
  public OIdentifier propertyType;
  public OIdentifier linkedType;
  public boolean unsafe = false;
  public List<OCreatePropertyAttributeStatement> attributes =
      new ArrayList<OCreatePropertyAttributeStatement>();

  public OCreatePropertyStatement(int id) {
    super(id);
  }

  public OCreatePropertyStatement(OrientSql p, int id) {
    super(p, id);
  }

  public void addAttribute(OCreatePropertyAttributeStatement attribute) {
    if (this.attributes == null) {
      this.attributes = new ArrayList<>();
    }
    this.attributes.add(attribute);
  }

  @Override
  public OExecutionStream executeDDL(OCommandContext ctx) {
    OResultInternal result = new OResultInternal();
    result.setProperty("operation", "create property");
    result.setProperty("className", className.getStringValue());
    result.setProperty("propertyName", propertyName.getStringValue());
    executeInternal(ctx, result);
    return OExecutionStream.singleton(result);
  }

  private void executeInternal(OCommandContext ctx, OResultInternal result) {
    var db = ctx.getDatabase();
    OClassEmbedded clazz =
        (OClassEmbedded) db.getMetadata().getSchema().getClass(className.getStringValue());
    if (clazz == null) {
      throw new OCommandExecutionException("Class not found: " + className.getStringValue());
    }
    if (clazz.getProperty(propertyName.getStringValue()) != null) {
      if (ifNotExists) {
        return;
      }
      throw new OCommandExecutionException(
          "Property "
              + className.getStringValue()
              + "."
              + propertyName.getStringValue()
              + " already exists");
    }
    OType type = OType.valueOf(propertyType.getStringValue().toUpperCase(Locale.ENGLISH));
    if (type == null) {
      throw new OCommandExecutionException(
          "Invalid property type: " + propertyType.getStringValue());
    }
    OClass linkedClass = null;
    OType linkedType = null;
    if (this.linkedType != null) {
      String linked = this.linkedType.getStringValue();
      // FIRST SEARCH BETWEEN CLASSES
      linkedClass = db.getMetadata().getSchema().getClass(linked);
      if (linkedClass == null)
      // NOT FOUND: SEARCH BETWEEN TYPES
      {
        linkedType = OType.valueOf(linked.toUpperCase(Locale.ENGLISH));
      }
    }
    // CREATE IT LOCALLY
    OPropertyImpl internalProp =
        (OPropertyImpl)
            clazz.addProperty(propertyName.getStringValue(), type, linkedType, linkedClass, unsafe);
    for (OCreatePropertyAttributeStatement attr : attributes) {
      Object val = attr.setOnProperty(internalProp, ctx);
      result.setProperty(attr.settingName.getStringValue(), val);
    }
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    builder.append("CREATE PROPERTY ");
    className.toString(params, builder);
    builder.append(".");
    propertyName.toString(params, builder);
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }
    builder.append(" ");
    propertyType.toString(params, builder);
    if (linkedType != null) {
      builder.append(" ");
      linkedType.toString(params, builder);
    }

    if (!attributes.isEmpty()) {
      builder.append(" (");
      for (int i = 0; i < attributes.size(); i++) {
        OCreatePropertyAttributeStatement att = attributes.get(i);
        att.toString(params, builder);

        if (i < attributes.size() - 1) {
          builder.append(", ");
        }
      }
      builder.append(")");
    }

    if (unsafe) {
      builder.append(" UNSAFE");
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    builder.append("CREATE PROPERTY ");
    className.toGenericStatement(builder);
    builder.append(".");
    propertyName.toGenericStatement(builder);
    if (ifNotExists) {
      builder.append(" IF NOT EXISTS");
    }
    builder.append(" ");
    propertyType.toGenericStatement(builder);
    if (linkedType != null) {
      builder.append(" ");
      linkedType.toGenericStatement(builder);
    }

    if (!attributes.isEmpty()) {
      builder.append(" (");
      for (int i = 0; i < attributes.size(); i++) {
        OCreatePropertyAttributeStatement att = attributes.get(i);
        att.toGenericStatement(builder);

        if (i < attributes.size() - 1) {
          builder.append(", ");
        }
      }
      builder.append(")");
    }

    if (unsafe) {
      builder.append(" UNSAFE");
    }
  }

  @Override
  public OCreatePropertyStatement copy() {
    OCreatePropertyStatement result = new OCreatePropertyStatement(-1);
    result.className = className == null ? null : className.copy();
    result.propertyName = propertyName == null ? null : propertyName.copy();
    result.propertyType = propertyType == null ? null : propertyType.copy();
    result.linkedType = linkedType == null ? null : linkedType.copy();
    result.unsafe = unsafe;
    result.ifNotExists = ifNotExists;
    result.attributes =
        attributes == null
            ? null
            : attributes.stream().map(x -> x.copy()).collect(Collectors.toList());
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OCreatePropertyStatement that = (OCreatePropertyStatement) o;

    if (unsafe != that.unsafe) {
      return false;
    }
    if (!Objects.equals(className, that.className)) {
      return false;
    }
    if (!Objects.equals(propertyName, that.propertyName)) {
      return false;
    }
    if (!Objects.equals(propertyType, that.propertyType)) {
      return false;
    }
    if (!Objects.equals(linkedType, that.linkedType)) {
      return false;
    }
    if (!Objects.equals(attributes, that.attributes)) {
      return false;
    }
    return ifNotExists == that.ifNotExists;
  }

  @Override
  public int hashCode() {
    int result = className != null ? className.hashCode() : 0;
    result = 31 * result + (propertyName != null ? propertyName.hashCode() : 0);
    result = 31 * result + (propertyType != null ? propertyType.hashCode() : 0);
    result = 31 * result + (linkedType != null ? linkedType.hashCode() : 0);
    result = 31 * result + (unsafe ? 1 : 0);
    result = 31 * result + (attributes != null ? attributes.hashCode() : 0);
    return result;
  }
}
/* JavaCC - OriginalChecksum=ff78676483d59013ab10b13bde2678d3 (do not edit this line) */
