/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.parser.BaseParser;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerCSVAbstract;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItem;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItemField;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItemParameter;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItemVariable;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLPredicate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionRuntime;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * SQL Helper class
 */
public class SQLHelper {

  public static final String NAME = "sql";

  public static final String VALUE_NOT_PARSED = "_NOT_PARSED_";
  public static final String NOT_NULL = "_NOT_NULL_";
  public static final String DEFINED = "_DEFINED_";

  public static Object parseDefaultValue(DatabaseSessionInternal session, EntityImpl iRecord,
      final String iWord) {
    final var v = SQLHelper.parseValue(iWord, null);

    if (v != VALUE_NOT_PARSED) {
      return v;
    }

    // TRY TO PARSE AS FUNCTION
    final var func = SQLHelper.getFunction(session, null, iWord);
    if (func != null) {
      var context = new BasicCommandContext();
      context.setDatabaseSession(session);

      return func.execute(iRecord, iRecord, null, context);
    }

    // PARSE AS FIELD
    return iWord;
  }

  /**
   * Convert fields from text to real value. Supports: String, RID, Boolean, Float, Integer and
   * NULL.
   *
   * @param iValue Value to convert.
   * @return The value converted if recognized, otherwise VALUE_NOT_PARSED
   */
  public static Object parseValue(String iValue, final CommandContext iContext) {
    return parseValue(iValue, iContext, false);
  }

  public static Object parseValue(
      String iValue, final CommandContext iContext, boolean resolveContextVariables) {

    if (iValue == null) {
      return null;
    }

    iValue = iValue.trim();

    Object fieldValue = VALUE_NOT_PARSED;

    if (iValue.length() == 0) {
      return iValue;
    }
    if (iValue.startsWith("'") && iValue.endsWith("'")
        || iValue.startsWith("\"") && iValue.endsWith("\""))
    // STRING
    {
      fieldValue = IOUtils.getStringContent(iValue);
    } else if (iValue.charAt(0) == StringSerializerHelper.LIST_BEGIN
        && iValue.charAt(iValue.length() - 1) == StringSerializerHelper.LIST_END) {
      // COLLECTION/ARRAY
      final var items =
          StringSerializerHelper.smartSplit(
              iValue.substring(1, iValue.length() - 1), StringSerializerHelper.RECORD_SEPARATOR);

      final List<Object> coll = new ArrayList<Object>();
      for (var item : items) {
        coll.add(parseValue(item, iContext, resolveContextVariables));
      }
      fieldValue = coll;

    } else if (iValue.charAt(0) == StringSerializerHelper.MAP_BEGIN
        && iValue.charAt(iValue.length() - 1) == StringSerializerHelper.MAP_END) {
      // MAP
      final var items =
          StringSerializerHelper.smartSplit(
              iValue.substring(1, iValue.length() - 1), StringSerializerHelper.RECORD_SEPARATOR);

      final Map<Object, Object> map = new HashMap<Object, Object>();
      for (var item : items) {
        final var parts =
            StringSerializerHelper.smartSplit(item, StringSerializerHelper.ENTRY_SEPARATOR);

        if (parts == null || parts.size() != 2) {
          throw new CommandSQLParsingException(iContext.getDatabaseSession().getDatabaseName(),
              "Map found but entries are not defined as <key>:<value>");
        }

        Object key = StringSerializerHelper.decode(parseValue(parts.get(0), iContext).toString());
        var value = parseValue(parts.get(1), iContext);
        if (VALUE_NOT_PARSED == value) {
          value = new SQLPredicate(iContext, parts.get(1)).evaluate(iContext);
        }
        if (value instanceof String) {
          value = StringSerializerHelper.decode(value.toString());
        }
        map.put(key, value);
      }

      if (map.containsKey(EntityHelper.ATTRIBUTE_TYPE))
      // IT'S A DOCUMENT
      // TODO: IMPROVE THIS CASE AVOIDING DOUBLE PARSING
      {
        var entity = new EntityImpl(iContext.getDatabaseSession());
        entity.updateFromJSON(iValue);
        fieldValue = entity;
      } else {
        fieldValue = map;
      }

    } else if (iValue.charAt(0) == StringSerializerHelper.EMBEDDED_BEGIN
        && iValue.charAt(iValue.length() - 1) == StringSerializerHelper.EMBEDDED_END) {
      // SUB-COMMAND
      fieldValue = new CommandSQL(iValue.substring(1, iValue.length() - 1));
      ((CommandSQL) fieldValue).getContext().setParent(iContext);

    } else if (RecordId.isA(iValue))
    // RID
    {
      fieldValue = new RecordId(iValue.trim());
    } else {

      if (iValue.equalsIgnoreCase("null"))
      // NULL
      {
        fieldValue = null;
      } else if (iValue.equalsIgnoreCase("not null"))
      // NULL
      {
        fieldValue = NOT_NULL;
      } else if (iValue.equalsIgnoreCase("defined"))
      // NULL
      {
        fieldValue = DEFINED;
      } else if (iValue.equalsIgnoreCase("true"))
      // BOOLEAN, TRUE
      {
        fieldValue = Boolean.TRUE;
      } else if (iValue.equalsIgnoreCase("false"))
      // BOOLEAN, FALSE
      {
        fieldValue = Boolean.FALSE;
      } else if (iValue.startsWith("date(")) {
        final var func = SQLHelper.getFunction(iContext.getDatabaseSession(), null,
            iValue);
        if (func != null) {
          fieldValue = func.execute(null, null, null, iContext);
        }
      } else if (resolveContextVariables && iValue.startsWith("$") && iContext != null) {
        fieldValue = iContext.getVariable(iValue);
      } else {
        final var v = parseStringNumber(iValue);
        if (v != null) {
          fieldValue = v;
        }
      }
    }

    return fieldValue;
  }

  public static Object parseStringNumber(final String iValue) {
    final var t = RecordSerializerCSVAbstract.getType(iValue);

    if (t == PropertyType.INTEGER) {
      return Integer.parseInt(iValue);
    } else if (t == PropertyType.LONG) {
      return Long.parseLong(iValue);
    } else if (t == PropertyType.FLOAT) {
      return Float.parseFloat(iValue);
    } else if (t == PropertyType.SHORT) {
      return Short.parseShort(iValue);
    } else if (t == PropertyType.BYTE) {
      return Byte.parseByte(iValue);
    } else if (t == PropertyType.DOUBLE) {
      return Double.parseDouble(iValue);
    } else if (t == PropertyType.DECIMAL) {
      return new BigDecimal(iValue);
    } else if (t == PropertyType.DATE || t == PropertyType.DATETIME) {
      return new Date(Long.parseLong(iValue));
    }

    return null;
  }

  public static Object parseValue(
      final SQLPredicate iSQLFilter,
      final BaseParser iCommand,
      final String iWord,
      @Nonnull final CommandContext iContext) {
    if (iWord.charAt(0) == StringSerializerHelper.PARAMETER_POSITIONAL
        || iWord.charAt(0) == StringSerializerHelper.PARAMETER_NAMED) {
      if (iSQLFilter != null) {
        return iSQLFilter.addParameter(iWord);
      } else {
        return new SQLFilterItemParameter(iWord);
      }
    } else {
      return parseValue(iCommand, iWord, iContext);
    }
  }

  public static Object parseValue(
      final BaseParser iCommand, final String iWord, final CommandContext iContext) {
    return parseValue(iCommand, iWord, iContext, false);
  }

  public static Object parseValue(
      final BaseParser iCommand,
      final String iWord,
      final CommandContext iContext,
      boolean resolveContextVariables) {
    if (iWord.equals("*")) {
      return "*";
    }

    // TRY TO PARSE AS RAW VALUE
    final var v = parseValue(iWord, iContext, resolveContextVariables);
    if (v != VALUE_NOT_PARSED) {
      return v;
    }

    if (!iWord.equalsIgnoreCase("any()") && !iWord.equalsIgnoreCase("all()")) {
      // TRY TO PARSE AS FUNCTION
      final Object func = SQLHelper.getFunction(iContext.getDatabaseSession(), iCommand, iWord);
      if (func != null) {
        return func;
      }
    }

    if (iWord.startsWith("$"))
    // CONTEXT VARIABLE
    {
      return new SQLFilterItemVariable(iContext.getDatabaseSession(), iCommand, iWord);
    }

    // PARSE AS FIELD
    return new SQLFilterItemField(iContext.getDatabaseSession(), iCommand, iWord, null);
  }

  public static SQLFunctionRuntime getFunction(DatabaseSessionInternal session,
      final BaseParser iCommand, final String iWord) {
    final var separator = iWord.indexOf('.');
    final var beginParenthesis = iWord.indexOf(StringSerializerHelper.EMBEDDED_BEGIN);
    if (beginParenthesis > -1 && (separator == -1 || separator > beginParenthesis)) {
      final var endParenthesis =
          iWord.indexOf(StringSerializerHelper.EMBEDDED_END, beginParenthesis);

      final var firstChar = iWord.charAt(0);
      if (endParenthesis > -1 && (firstChar == '_' || Character.isLetter(firstChar)))
      // FUNCTION: CREATE A RUN-TIME CONTAINER FOR IT TO SAVE THE PARAMETERS
      {
        return new SQLFunctionRuntime(session, iCommand, iWord);
      }
    }

    return null;
  }

  public static Object getValue(final Object iObject) {
    if (iObject == null) {
      return null;
    }

    if (iObject instanceof SQLFilterItem) {
      return ((SQLFilterItem) iObject).getValue(null, null, null);
    }

    return iObject;
  }

  public static Object getValue(
      final Object iObject, final DBRecord iRecord, final CommandContext iContext) {
    if (iObject == null) {
      return null;
    }

    if (iObject instanceof SQLFilterItem) {
      return ((SQLFilterItem) iObject).getValue(iRecord, null, iContext);
    } else if (iObject instanceof String) {
      final var s = ((String) iObject).trim();
      if (iRecord != null & !s.isEmpty()
          && !IOUtils.isStringContent(iObject)
          && !Character.isDigit(s.charAt(0)))
      // INTERPRETS IT
      {
        return EntityHelper.getFieldValue(iContext.getDatabaseSession(), iRecord, s, iContext);
      }
    }

    return iObject;
  }

  public static Object resolveFieldValue(
      DatabaseSession session, final EntityImpl entity,
      final String iFieldName,
      final Object iFieldValue,
      final CommandParameters iArguments,
      final CommandContext iContext) {
    if (iFieldValue instanceof SQLFilterItemField f) {
      if (f.getRoot(session).equals("?"))
      // POSITIONAL PARAMETER
      {
        return iArguments.getNext();
      } else if (f.getRoot(session).startsWith(":"))
      // NAMED PARAMETER
      {
        return iArguments.getByName(f.getRoot(session).substring(1));
      }
    }

    if (iFieldValue instanceof EntityImpl && !((EntityImpl) iFieldValue).getIdentity()
        .isValid())
    // EMBEDDED entity
    {
      ((EntityImpl) iFieldValue).addOwner(entity);
    }

    // can't use existing getValue with iContext
    if (iFieldValue == null) {
      return null;
    }
    if (iFieldValue instanceof SQLFilterItem) {
      return ((SQLFilterItem) iFieldValue).getValue(entity, null, iContext);
    }

    return iFieldValue;
  }

  public static EntityImpl bindParameters(
      final EntityImpl entity,
      final Map<String, Object> iFields,
      final CommandParameters iArguments,
      final CommandContext iContext) {
    if (iFields == null) {
      return null;
    }

    final List<Pair<String, Object>> fields = new ArrayList<Pair<String, Object>>(iFields.size());

    for (var entry : iFields.entrySet()) {
      fields.add(new Pair<String, Object>(entry.getKey(), entry.getValue()));
    }

    return bindParameters(entity, fields, iArguments, iContext);
  }

  public static EntityImpl bindParameters(
      final EntityImpl e,
      final List<Pair<String, Object>> iFields,
      final CommandParameters iArguments,
      final CommandContext iContext) {
    if (iFields == null) {
      return null;
    }

    var session = iContext.getDatabaseSession();
    // BIND VALUES
    for (var field : iFields) {
      final var fieldName = field.getKey();
      var fieldValue = field.getValue();

      if (fieldValue != null) {
        if (fieldValue instanceof CommandSQL cmd) {
          cmd.getContext().setParent(iContext);
          fieldValue = session.command(cmd)
              .execute(iContext.getDatabaseSession());

          // CHECK FOR CONVERSIONS
          SchemaImmutableClass immutableClass = null;
          if (e != null) {
            immutableClass = e.getImmutableSchemaClass(session);
          }
          if (immutableClass != null) {
            final var prop = immutableClass.getProperty(session, fieldName);
            if (prop != null) {
              if (prop.getType(session) == PropertyType.LINK) {
                if (MultiValue.isMultiValue(fieldValue)) {
                  final var size = MultiValue.getSize(fieldValue);
                  if (size == 1)
                  // GET THE FIRST ITEM AS UNIQUE LINK
                  {
                    fieldValue = MultiValue.getFirstValue(fieldValue);
                  } else if (size == 0)
                  // NO ITEMS, SET IT AS NULL
                  {
                    fieldValue = null;
                  }
                }
              }
            } else if (immutableClass.isEdgeType(session)
                && ("out".equals(fieldName) || "in".equals(fieldName))
                && (fieldValue instanceof List lst)) {
              if (lst.size() == 1) {
                fieldValue = lst.get(0);
              }
            }
          }

          if (MultiValue.isMultiValue(fieldValue)) {
            final List<Object> tempColl = new ArrayList<Object>(MultiValue.getSize(fieldValue));

            String singleFieldName = null;
            for (var o : MultiValue.getMultiValueIterable(fieldValue)) {
              if (o instanceof Identifiable && !((Identifiable) o).getIdentity()
                  .isPersistent()) {
                // TEMPORARY / EMBEDDED
                var rec = ((Identifiable) o).getRecord(session);
                if (rec != null && rec instanceof EntityImpl entity) {
                  // CHECK FOR ONE FIELD ONLY
                  if (entity.fields() == 1) {
                    singleFieldName = entity.fieldNames()[0];
                    tempColl.add(entity.field(singleFieldName));
                  } else {
                    // TRANSFORM IT IN EMBEDDED
                    entity.getIdentity().reset();
                    entity.addOwner(e);
                    entity.addOwner(e);
                    tempColl.add(entity);
                  }
                }
              } else {
                tempColl.add(o);
              }
            }

            fieldValue = tempColl;
          }
        }
      }

      e.field(
          fieldName,
          resolveFieldValue(iContext.getDatabaseSession(), e, fieldName, fieldValue, iArguments,
              iContext));
    }
    return e;
  }
}
