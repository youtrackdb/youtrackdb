/* Generated By:JJTree: Do not edit this line. OCreatePropertyAttributeStatement.java Version 4.3 */
/* JavaCCOptions:MULTI=true,NODE_USES_PARSER=false,VISITOR=true,TRACK_TOKENS=true,NODE_PREFIX=O,NODE_EXTENDS=,NODE_FACTORY=,SUPPORT_CLASS_VISIBILITY_PUBLIC=true */
package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OPropertyImpl;
import java.util.Map;
import java.util.Objects;

public class OCreatePropertyAttributeStatement extends SimpleNode {

  public OIdentifier settingName;
  public OExpression settingValue;

  public OCreatePropertyAttributeStatement(int id) {
    super(id);
  }

  public OCreatePropertyAttributeStatement(OrientSql p, int id) {
    super(p, id);
  }

  @Override
  public void toString(Map<Object, Object> params, StringBuilder builder) {
    settingName.toString(params, builder);
    if (settingValue != null) {
      builder.append(" ");
      settingValue.toString(params, builder);
    }
  }

  @Override
  public void toGenericStatement(StringBuilder builder) {
    settingName.toGenericStatement(builder);
    if (settingValue != null) {
      builder.append(" ");
      settingValue.toGenericStatement(builder);
    }
  }

  public OCreatePropertyAttributeStatement copy() {
    OCreatePropertyAttributeStatement result = new OCreatePropertyAttributeStatement(-1);
    result.settingName = settingName == null ? null : settingName.copy();
    result.settingValue = settingValue == null ? null : settingValue.copy();
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

    OCreatePropertyAttributeStatement that = (OCreatePropertyAttributeStatement) o;

    if (!Objects.equals(settingName, that.settingName)) {
      return false;
    }
    return Objects.equals(settingValue, that.settingValue);
  }

  @Override
  public int hashCode() {
    int result = settingName != null ? settingName.hashCode() : 0;
    result = 31 * result + (settingValue != null ? settingValue.hashCode() : 0);
    return result;
  }

  public Object setOnProperty(OPropertyImpl internalProp, OCommandContext ctx) {
    String attrName = settingName.getStringValue();
    Object attrValue =
        this.settingValue == null ? true : this.settingValue.execute((OIdentifiable) null, ctx);
    try {
      if (attrName.equalsIgnoreCase("readonly")) {
        internalProp.setReadonly((boolean) attrValue);
      } else if (attrName.equalsIgnoreCase("mandatory")) {
        internalProp.setMandatory((boolean) attrValue);
      } else if (attrName.equalsIgnoreCase("notnull")) {
        internalProp.setNotNull((boolean) attrValue);
      } else if (attrName.equalsIgnoreCase("max")) {
        internalProp.setMax("" + attrValue);
      } else if (attrName.equalsIgnoreCase("min")) {
        internalProp.setMin("" + attrValue);
      } else if (attrName.equalsIgnoreCase("default")) {
        if (this.settingValue == null) {
          throw new OCommandExecutionException("");
        }
        internalProp.setDefaultValue("" + attrValue);
      } else if (attrName.equalsIgnoreCase("collate")) {
        internalProp.setCollate("" + attrValue);
      } else if (attrName.equalsIgnoreCase("regexp")) {
        internalProp.setRegexp("" + attrValue);
      } else {
        throw new OCommandExecutionException("Invalid attribute definition: '" + attrName + "'");
      }
    } catch (Exception e) {
      throw OException.wrapException(
          new OCommandExecutionException(
              "Cannot set attribute on property " + settingName.getStringValue() + " " + attrValue),
          e);
    }
    return attrValue;
  }
}
/* JavaCC - OriginalChecksum=6a7964c2b9dad541ca962eecea00651b (do not edit this line) */
