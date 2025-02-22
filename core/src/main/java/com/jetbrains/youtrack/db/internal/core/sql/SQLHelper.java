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
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.parser.BaseParser;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerCSVAbstract;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerJackson;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItem;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItemField;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItemParameter;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterItemVariable;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLPredicate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionRuntime;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * SQL Helper class
 */
public class SQLHelper {

  public static final String NAME = "sql";

  public static final String VALUE_NOT_PARSED = "_NOT_PARSED_";
  public static final String NOT_NULL = "_NOT_NULL_";
  public static final String DEFINED = "_DEFINED_";

  public static Object parseDefaultValue(DatabaseSessionInternal session, EntityImpl iRecord,
      final String iWord, @Nonnull SchemaProperty schemaProperty) {
    var context = new BasicCommandContext();
    context.setDatabaseSession(session);

    final var v = SQLHelper.parseValue(iWord, context, schemaProperty);

    if (v != VALUE_NOT_PARSED) {
      return v;
    }

    // TRY TO PARSE AS FUNCTION
    final var func = SQLHelper.getFunction(session, null, iWord);
    if (func != null) {
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
  public static Object parseValue(String iValue, final CommandContext context,
      @Nullable SchemaProperty schemaProperty) {
    return parseValue(iValue, context, false,
        null, schemaProperty, null, null);
  }

  public static Object parseValue(
      String iValue, final CommandContext context,
      boolean resolveContextVariables,
      @Nullable SchemaClass schemaClass,
      @Nullable SchemaProperty schemaProperty, @Nullable PropertyType propertyType,
      @Nullable PropertyType parentProperty) {
    if (iValue == null) {
      return null;
    }

    if (propertyType == null && schemaProperty != null) {
      propertyType = schemaProperty.getType(context.getDatabaseSession());
    }

    iValue = iValue.trim();

    Object fieldValue = VALUE_NOT_PARSED;

    var session = context.getDatabaseSession();
    if (iValue.isEmpty()) {
      return iValue;
    }

    if (iValue.charAt(0) == '\'' && iValue.charAt(iValue.length() - 1) == '\''
        || iValue.charAt(0) == '\"'
        && iValue.charAt(iValue.length() - 1) == '\"')
    // STRING
    {
      fieldValue = IOUtils.getStringContent(iValue);
    } else if (iValue.charAt(0) == StringSerializerHelper.LIST_BEGIN
        && iValue.charAt(iValue.length() - 1) == StringSerializerHelper.LIST_END) {
      // COLLECTION/ARRAY
      final var items =
          StringSerializerHelper.smartSplit(
              iValue.substring(1, iValue.length() - 1), StringSerializerHelper.RECORD_SEPARATOR);

      List<Object> coll;
      if (propertyType != null) {
        if (propertyType.isMultiValue()) {
          if (propertyType.isLink()) {
            //noinspection rawtypes,unchecked
            coll = (List) session.newLinkList();
          } else {
            coll = session.newEmbeddedList();
          }
        } else {
          throw new IllegalArgumentException(
              "Value is a list but property is not a collection : " + iValue);
        }
      } else {
        coll = session.newEmbeddedList();
      }

      if (schemaProperty != null) {
        var linkedType = schemaProperty.getLinkedType(session);
        var linkedClass = schemaProperty.getLinkedClass(session);
        for (var item : items) {
          coll.add(parseValue(item, context, resolveContextVariables,
              linkedClass,
              null, linkedType, propertyType));
        }
      } else {
        for (var item : items) {
          coll.add(parseValue(item, context, resolveContextVariables, null, null, null,
              null));
        }
      }

      fieldValue = coll;

    } else if (iValue.charAt(0) == StringSerializerHelper.MAP_BEGIN
        && iValue.charAt(iValue.length() - 1) == StringSerializerHelper.MAP_END) {
      // map or entity
      if (schemaClass != null) {
        Entity entity;
        if (parentProperty != null) {
          if (parentProperty.isEmbedded()) {
            entity = session.newEmbededEntity(schemaClass);
          } else if (parentProperty.isLink()) {
            entity = session.newEntity(schemaClass);
          } else {
            throw new IllegalArgumentException(
                "Property is not a link or embedded : " + parentProperty);
          }
        } else {
          entity = session.newEntity(schemaClass);
        }
        fieldValue = RecordSerializerJackson.fromString(session, iValue, (RecordAbstract) entity);
      } else {
        final var items =
            StringSerializerHelper.smartSplit(
                iValue.substring(1, iValue.length() - 1), StringSerializerHelper.RECORD_SEPARATOR);

        Map<String, Object> map;
        if (propertyType != null) {
          if (propertyType.isMultiValue()) {
            if (propertyType.isLink()) {
              //noinspection unchecked,rawtypes
              map = (Map) session.newLinkMap();
            } else {
              map = session.newEmbeddedMap();
            }
          } else {
            throw new IllegalArgumentException(
                "Value is a map but property is not a collection : " + iValue);
          }
        } else {
          map = session.newEmbeddedMap();
        }

        for (var item : items) {
          final var parts =
              StringSerializerHelper.smartSplit(item, StringSerializerHelper.ENTRY_SEPARATOR);

          if (parts.size() != 2) {
            throw new CommandSQLParsingException(context.getDatabaseSession().getDatabaseName(),
                "Map found but entries are not defined as <key>:<value>");
          }

          Object key;
          Object value;
          if (schemaProperty != null) {
            var linkedType = schemaProperty.getLinkedType(session);
            var linkedClass = schemaProperty.getLinkedClass(session);
            key = parseValue(parts.get(0), context, resolveContextVariables,
                null,
                null, PropertyType.STRING, propertyType);
            value = parseValue(parts.get(1), context, resolveContextVariables,
                linkedClass,
                null, linkedType, propertyType);
          } else {
            key = parseValue(parts.get(0), context, resolveContextVariables,
                null, null, null, null);
            value = parseValue(parts.get(1), context, resolveContextVariables,
                null, null, null, null);
          }

          if (VALUE_NOT_PARSED == value) {
            value = new SQLPredicate(context, parts.get(1)).evaluate(context);
          }
          if (value instanceof String) {
            value = StringSerializerHelper.decode(value.toString());
          }
          map.put(key.toString(), value);
        }

        if (map.containsKey(EntityHelper.ATTRIBUTE_TYPE))
        // entity
        {
          Entity entity;
          if (parentProperty != null) {
            if (parentProperty.isEmbedded()) {
              entity = session.newEmbededEntity();
            } else if (parentProperty.isLink()) {
              entity = session.newEntity();
            } else {
              throw new IllegalArgumentException(
                  "Property is not a link or embedded : " + parentProperty);
            }
          } else {
            entity = session.newEntity();
          }

          fieldValue = RecordSerializerJackson.fromString(session, iValue, (RecordAbstract) entity);
        } else {
          fieldValue = map;
        }
      }
    } else if (iValue.charAt(0) == StringSerializerHelper.EMBEDDED_BEGIN
        && iValue.charAt(iValue.length() - 1) == StringSerializerHelper.EMBEDDED_END) {
      // SUB-COMMAND
      fieldValue = new CommandSQL(iValue.substring(1, iValue.length() - 1));
      ((CommandSQL) fieldValue).getContext().setParent(context);

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
        final var func = SQLHelper.getFunction(context.getDatabaseSession(), null,
            iValue);
        if (func != null) {
          fieldValue = func.execute(null, null, null, context);
        }
      } else if (resolveContextVariables && iValue.charAt(0) == '$') {
        fieldValue = context.getVariable(iValue);
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
      final CommandContext context,
      boolean resolveContextVariables) {
    if (iWord.equals("*")) {
      return "*";
    }

    List<SchemaProperty> updatedPropertyStack = context.getSystemVariable(
        CommandContext.SQL_UPDATED_PROPERTY_STACK_SYSTEM_VARIABLE);
    SchemaProperty schemaProperty = null;
    if (updatedPropertyStack != null && !updatedPropertyStack.isEmpty()) {
      schemaProperty = updatedPropertyStack.getLast();
    }

    // TRY TO PARSE AS RAW VALUE
    final var v = parseValue(iWord, context, resolveContextVariables, null,
        schemaProperty, null, null);
    if (v != VALUE_NOT_PARSED) {
      return v;
    }

    if (!iWord.equalsIgnoreCase("any()") && !iWord.equalsIgnoreCase("all()")) {
      // TRY TO PARSE AS FUNCTION
      final Object func = SQLHelper.getFunction(context.getDatabaseSession(), iCommand, iWord);
      if (func != null) {
        return func;
      }
    }

    if (!iWord.isEmpty() && iWord.charAt(0) == '$')
    // CONTEXT VARIABLE
    {
      return new SQLFilterItemVariable(context.getDatabaseSession(), iCommand, iWord);
    }

    // PARSE AS FIELD
    return new SQLFilterItemField(context.getDatabaseSession(), iCommand, iWord, null);
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
      ((EntityImpl) iFieldValue).setOwner(entity);
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
                    entity.setOwner(e);
                    entity.setOwner(e);
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
