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
package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.common.collection.OMultiValue;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.io.OIOUtils;
import com.jetbrains.youtrack.db.internal.common.util.OPair;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.config.OStorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.YTQueryParsingException;
import com.jetbrains.youtrack.db.internal.core.exception.YTRecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.ORecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.OStringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.ORecordSerializerStringAbstract;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.OSQLHelper;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLPredicate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionRuntime;
import com.jetbrains.youtrack.db.internal.core.sql.method.OSQLMethod;
import com.jetbrains.youtrack.db.internal.core.util.ODateHelper;
import java.lang.reflect.Array;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Helper class to manage documents.
 */
public class ODocumentHelper {

  public static final String ATTRIBUTE_THIS = "@this";
  public static final String ATTRIBUTE_RID = "@rid";
  public static final String ATTRIBUTE_RID_ID = "@rid_id";
  public static final String ATTRIBUTE_RID_POS = "@rid_pos";
  public static final String ATTRIBUTE_VERSION = "@version";
  public static final String ATTRIBUTE_CLASS = "@class";
  public static final String ATTRIBUTE_TYPE = "@type";
  public static final String ATTRIBUTE_SIZE = "@size";
  public static final String ATTRIBUTE_FIELDS = "@fields";
  public static final String ATTRIBUTE_FIELS_TYPES = "@fieldtypes";
  public static final String ATTRIBUTE_RAW = "@raw";

  public interface ODbRelatedCall<T> {

    T call(YTDatabaseSessionInternal database);
  }

  public interface RIDMapper {

    YTRID map(YTRID rid);
  }

  public static Set<String> getReservedAttributes() {
    Set<String> retSet = new HashSet<>();
    retSet.add(ATTRIBUTE_THIS);
    retSet.add(ATTRIBUTE_RID);
    retSet.add(ATTRIBUTE_RID_ID);
    retSet.add(ATTRIBUTE_RID_POS);
    retSet.add(ATTRIBUTE_VERSION);
    retSet.add(ATTRIBUTE_CLASS);
    retSet.add(ATTRIBUTE_TYPE);
    retSet.add(ATTRIBUTE_SIZE);
    retSet.add(ATTRIBUTE_FIELDS);
    retSet.add(ATTRIBUTE_RAW);
    retSet.add(ATTRIBUTE_FIELS_TYPES);
    return retSet;
  }

  public static void sort(
      List<? extends YTIdentifiable> ioResultSet,
      List<OPair<String, String>> iOrderCriteria,
      CommandContext context) {
    if (ioResultSet != null) {
      ioResultSet.sort(new ODocumentComparator(iOrderCriteria, context));
    }
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET convertField(
      YTDatabaseSessionInternal session, final EntityImpl iDocument,
      final String iFieldName,
      YTType type,
      Class<?> iFieldType,
      Object iValue) {
    if (iFieldType == null && type != null) {
      iFieldType = type.getDefaultJavaType();
    }

    if (iFieldType == null) {
      return (RET) iValue;
    }

    if (YTRID.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof YTRID) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new YTRecordId((String) iValue);
      } else if (iValue instanceof Record) {
        return (RET) ((Record) iValue).getIdentity();
      }
    } else if (YTIdentifiable.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof YTRID || iValue instanceof Record) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new YTRecordId((String) iValue);
      } else if (OMultiValue.isMultiValue(iValue) && OMultiValue.getSize(iValue) == 1) {
        Object val = OMultiValue.getFirstValue(iValue);
        if (val instanceof YTResult) {
          val = ((YTResult) val).getIdentity().orElse(null);
        }
        if (val instanceof YTIdentifiable) {
          return (RET) val;
        }
      }
    } else if (Set.class.isAssignableFrom(iFieldType)) {
      if (!(iValue instanceof Set)) {
        // CONVERT IT TO SET
        final Collection<?> newValue;
        if (type.isLink()) {
          newValue = new LinkSet(iDocument);
        } else {
          newValue = new TrackedSet<>(iDocument);
        }
        if (iValue instanceof Collection<?>) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
        } else if (iValue instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
        } else if (iValue instanceof String stringValue) {

          if (!stringValue.isEmpty()) {
            final String[] items = stringValue.split(",");
            for (String s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
        } else if (OMultiValue.isMultiValue(iValue)) {
          // GENERIC MULTI VALUE
          for (Object s : OMultiValue.getMultiValueIterable(iValue)) {
            ((Collection<Object>) newValue).add(s);
          }
        }
        return (RET) newValue;
      } else {
        return (RET) iValue;
      }
    } else if (List.class.isAssignableFrom(iFieldType)) {
      if (!(iValue instanceof List)) {
        // CONVERT IT TO LIST
        final Collection<?> newValue;

        if (type.isLink()) {
          newValue = new LinkList(iDocument);
        } else {
          newValue = new TrackedList<Object>(iDocument);
        }

        if (iValue instanceof Collection) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) iValue);
        } else if (iValue instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) iValue).values());
        } else if (iValue instanceof String stringValue) {

          if (!stringValue.isEmpty()) {
            final String[] items = stringValue.split(",");
            for (String s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
        } else if (OMultiValue.isMultiValue(iValue)) {
          // GENERIC MULTI VALUE
          for (Object s : OMultiValue.getMultiValueIterable(iValue)) {
            ((Collection<Object>) newValue).add(s);
          }
        }
        return (RET) newValue;
      } else {
        return (RET) iValue;
      }
    } else if (iValue instanceof Enum) {
      // ENUM
      if (Number.class.isAssignableFrom(iFieldType)) {
        iValue = ((Enum<?>) iValue).ordinal();
      } else {
        iValue = iValue.toString();
      }
      if (!(iValue instanceof String) && !iFieldType.isAssignableFrom(iValue.getClass())) {
        throw new IllegalArgumentException(
            "Property '"
                + iFieldName
                + "' of type '"
                + iFieldType
                + "' cannot accept value of type: "
                + iValue.getClass());
      }
    } else if (Date.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof String && ODatabaseRecordThreadLocal.instance().isDefined()) {
        YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().get();
        final OStorageConfiguration config = db.getStorageInfo().getConfiguration();

        DateFormat formatter;

        if (((String) iValue).length() > config.getDateFormat().length()) {
          // ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
          formatter = config.getDateTimeFormatInstance();
        } else {
          formatter = config.getDateFormatInstance();
        }

        try {
          Date newValue = formatter.parse((String) iValue);
          // _fieldValues.put(iFieldName, newValue);
          return (RET) newValue;
        } catch (ParseException pe) {
          final String dateFormat =
              ((String) iValue).length() > config.getDateFormat().length()
                  ? config.getDateTimeFormat()
                  : config.getDateFormat();
          throw YTException.wrapException(
              new YTQueryParsingException(
                  "Error on conversion of date '" + iValue + "' using the format: " + dateFormat),
              pe);
        }
      }
    }

    iValue = YTType.convert(session, iValue, iFieldType);

    return (RET) iValue;
  }

  public static <RET> RET getFieldValue(YTDatabaseSessionInternal session, Object value,
      final String iFieldName) {
    var context = new BasicCommandContext();
    context.setDatabase(session);

    return getFieldValue(session, value, iFieldName, context);
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET getFieldValue(
      YTDatabaseSessionInternal session, Object value, final String iFieldName,
      @Nonnull final CommandContext iContext) {
    if (value == null) {
      return null;
    }

    final int fieldNameLength = iFieldName.length();
    if (fieldNameLength == 0) {
      return (RET) value;
    }

    YTIdentifiable currentRecord = value instanceof YTIdentifiable ? (YTIdentifiable) value : null;

    int beginPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    int nextSeparatorPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    boolean firstInChain = true;
    do {
      char nextSeparator = ' ';
      for (; nextSeparatorPos < fieldNameLength; ++nextSeparatorPos) {
        nextSeparator = iFieldName.charAt(nextSeparatorPos);
        if (nextSeparator == '.' || nextSeparator == '[') {
          break;
        }
      }

      final String fieldName;
      if (nextSeparatorPos < fieldNameLength) {
        fieldName = iFieldName.substring(beginPos, nextSeparatorPos);
      } else {
        nextSeparator = ' ';
        if (beginPos > 0) {
          fieldName = iFieldName.substring(beginPos);
        } else {
          fieldName = iFieldName;
        }
      }

      if (nextSeparator == '[') {
        if (!fieldName.isEmpty()) {
          if (currentRecord != null) {
            value = getIdentifiableValue(currentRecord, fieldName);
          } else if (value instanceof Map<?, ?>) {
            value = getMapEntry(session, (Map<String, ?>) value, fieldName);
          } else if (OMultiValue.isMultiValue(value)) {
            final HashSet<Object> temp = new LinkedHashSet<Object>();
            for (Object o : OMultiValue.getMultiValueIterable(value)) {
              if (o instanceof YTIdentifiable) {
                Object r = getFieldValue(session, o, iFieldName);
                if (r != null) {
                  OMultiValue.add(temp, r);
                }
              }
            }
            value = temp;
          }
        }

        if (value == null) {
          return null;
        } else if (value instanceof YTIdentifiable) {
          currentRecord = (YTIdentifiable) value;
        }

        // final int end = iFieldName.indexOf(']', nextSeparatorPos);
        final int end = findClosingBracketPosition(iFieldName, nextSeparatorPos);
        if (end == -1) {
          throw new IllegalArgumentException("Missed closed ']'");
        }

        String indexPart = iFieldName.substring(nextSeparatorPos + 1, end);
        if (indexPart.isEmpty()) {
          return null;
        }

        nextSeparatorPos = end;

        if (value instanceof CommandContext) {
          value = ((CommandContext) value).getVariables();
        }

        if (value instanceof YTIdentifiable) {
          final Record record =
              currentRecord instanceof YTIdentifiable ? currentRecord.getRecord() : null;

          final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts =
              OStringSerializerHelper.smartSplit(
                  indexAsString, ',', OStringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final List<String> indexRanges =
              OStringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final List<String> indexCondition =
              OStringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
          // SINGLE VALUE
          {
            value = ((EntityImpl) record).field(indexAsString);
          } else if (indexParts.size() > 1) {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = ((EntityImpl) record).field(
                  OIOUtils.getStringContent(indexParts.get(i)));
            }
            value = values;
          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            String from = indexRanges.get(0);
            String to = indexRanges.get(1);

            final EntityImpl doc = (EntityImpl) record;

            final String[] fieldNames = doc.fieldNames();
            final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo =
                to != null && !to.isEmpty()
                    ? Math.min(Integer.parseInt(to), fieldNames.length - 1)
                    : fieldNames.length - 1;

            final Object[] values = new Object[rangeTo - rangeFrom + 1];

            for (int i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = doc.field(fieldNames[i]);
            }

            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue =
                ORecordSerializerStringAbstract.getTypeValue(session, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = OIOUtils.getStringContent(conditionFieldValue);
            }

            final Object fieldValue = getFieldValue(session, currentRecord, conditionFieldName);

            if (conditionFieldValue != null && fieldValue != null) {
              conditionFieldValue = YTType.convert(session, conditionFieldValue,
                  fieldValue.getClass());
            }

            if (fieldValue == null && !conditionFieldValue.equals("null")
                || fieldValue != null && !fieldValue.equals(conditionFieldValue)) {
              value = null;
            }
          }
        } else if (value instanceof Map<?, ?>) {
          final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts =
              OStringSerializerHelper.smartSplit(
                  indexAsString, ',', OStringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final List<String> indexRanges =
              OStringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final List<String> indexCondition =
              OStringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          final Map<String, ?> map = (Map<String, ?>) value;
          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
          // SINGLE VALUE
          {
            value = map.get(index);
          } else if (indexParts.size() > 1) {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = map.get(OIOUtils.getStringContent(indexParts.get(i)));
            }
            value = values;
          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            String from = indexRanges.get(0);
            String to = indexRanges.get(1);

            final List<String> fieldNames = new ArrayList<String>(map.keySet());
            final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo =
                to != null && !to.isEmpty()
                    ? Math.min(Integer.parseInt(to), fieldNames.size() - 1)
                    : fieldNames.size() - 1;

            final Object[] values = new Object[rangeTo - rangeFrom + 1];

            for (int i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = map.get(fieldNames.get(i));
            }

            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue =
                ORecordSerializerStringAbstract.getTypeValue(session, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = OIOUtils.getStringContent(conditionFieldValue);
            }

            final Object fieldValue = map.get(conditionFieldName);

            if (conditionFieldValue != null && fieldValue != null) {
              conditionFieldValue = YTType.convert(session, conditionFieldValue,
                  fieldValue.getClass());
            }

            if (fieldValue == null && !conditionFieldValue.equals("null")
                || fieldValue != null && !fieldValue.equals(conditionFieldValue)) {
              value = null;
            }
          }

        } else if (OMultiValue.isMultiValue(value)) {
          // MULTI VALUE
          final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts = OStringSerializerHelper.smartSplit(indexAsString, ',');
          final List<String> indexRanges = OStringSerializerHelper.smartSplit(indexAsString, '-');
          final List<String> indexCondition =
              OStringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          if (isFieldName(indexAsString)) {
            // SINGLE VALUE
            if (value instanceof Map<?, ?>) {
              value = getMapEntry(session, (Map<String, ?>) value, index);
            } else if (Character.isDigit(indexAsString.charAt(0))) {
              value = OMultiValue.getValue(value, Integer.parseInt(indexAsString));
            } else
            // FILTER BY FIELD
            {
              value = getFieldValue(session, value, indexAsString, iContext);
            }

          } else if (isListOfNumbers(indexParts)) {

            // MULTI VALUES
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = OMultiValue.getValue(value, Integer.parseInt(indexParts.get(i)));
            }
            if (indexParts.size() > 1) {
              value = values;
            } else {
              value = values[0];
            }

          } else if (isListOfNumbers(indexRanges)) {

            // MULTI VALUES RANGE
            String from = indexRanges.get(0);
            String to = indexRanges.get(1);

            final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo;
            if (to != null && !to.isEmpty()) {
              rangeTo = Math.min(Integer.parseInt(to), OMultiValue.getSize(value) - 1);
            } else {
              rangeTo = OMultiValue.getSize(value) - 1;
            }

            int arraySize = rangeTo - rangeFrom + 1;
            if (arraySize < 0) {
              arraySize = 0;
            }
            final Object[] values = new Object[arraySize];
            for (int i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = OMultiValue.getValue(value, i);
            }
            value = values;

          } else {
            // CONDITION
            SQLPredicate pred = new SQLPredicate(iContext, indexAsString);
            final HashSet<Object> values = new LinkedHashSet<Object>();

            for (Object v : OMultiValue.getMultiValueIterable(value)) {
              if (v instanceof YTIdentifiable) {
                Object result =
                    pred.evaluate((YTIdentifiable) v, ((YTIdentifiable) v).getRecord(), iContext);
                if (Boolean.TRUE.equals(result)) {
                  values.add(v);
                }
              } else if (v instanceof Map) {
                EntityImpl doc = new EntityImpl();
                doc.fromMap((Map<String, ? extends Object>) v);
                Object result = pred.evaluate(doc, doc, iContext);
                if (Boolean.TRUE.equals(result)) {
                  values.add(v);
                }
              }
            }

            if (values.isEmpty())
            // RETURNS NULL
            {
              value = values;
            } else if (values.size() == 1)
            // RETURNS THE SINGLE ODOCUMENT
            {
              value = values.iterator().next();
            } else
            // RETURNS THE FILTERED COLLECTION
            {
              value = values;
            }
          }
        }
      } else {
        if (fieldName.length() == 0) {
          // NO FIELD NAME: THIS IS THE CASE OF NOT USEFUL . AFTER A ] OR .
          beginPos = ++nextSeparatorPos;
          continue;
        }

        if (fieldName.startsWith("$")) {
          value = iContext.getVariable(fieldName);
        } else if (fieldName.contains("(")) {
          boolean executedMethod = false;
          if (!firstInChain && fieldName.endsWith("()")) {
            OSQLMethod method =
                OSQLEngine.getMethod(fieldName.substring(0, fieldName.length() - 2));
            if (method != null) {
              value = method.execute(value, currentRecord, iContext, value, new Object[]{});
              executedMethod = true;
            }
          }
          if (!executedMethod) {
            value = evaluateFunction(value, fieldName, iContext);
          }
        } else {
          final List<String> indexCondition =
              OStringSerializerHelper.smartSplit(fieldName, '=', ' ');

          if (indexCondition.size() == 2) {
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue =
                ORecordSerializerStringAbstract.getTypeValue(session, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = OIOUtils.getStringContent(conditionFieldValue);
            }

            value = filterItem(session, conditionFieldName, conditionFieldValue, value);

          } else if (currentRecord != null) {
            // GET THE LINKED OBJECT IF ANY
            value = getIdentifiableValue(currentRecord, fieldName);
          } else if (value instanceof Map<?, ?>) {
            value = getMapEntry(session, (Map<String, ?>) value, fieldName);
          } else if (OMultiValue.isMultiValue(value)) {
            final Set<Object> values = new LinkedHashSet<Object>();
            for (Object v : OMultiValue.getMultiValueIterable(value)) {
              final Object item;

              if (v instanceof YTIdentifiable) {
                item = getIdentifiableValue((YTIdentifiable) v, fieldName);
              } else if (v instanceof Map) {
                item = ((Map<?, ?>) v).get(fieldName);
              } else {
                item = null;
              }

              if (item != null) {
                if (item instanceof Collection<?>) {
                  values.addAll((Collection<? extends Object>) item);
                } else {
                  values.add(item);
                }
              }
            }

            if (values.isEmpty()) {
              value = null;
            } else {
              value = values;
            }
          } else {
            return null;
          }
        }
      }

      if (value instanceof YTIdentifiable) {
        currentRecord = (YTIdentifiable) value;
      } else {
        currentRecord = null;
      }

      beginPos = ++nextSeparatorPos;
      firstInChain = false;
    } while (nextSeparatorPos < fieldNameLength && value != null);

    return (RET) value;
  }

  private static int findClosingBracketPosition(String iFieldName, int nextSeparatorPos) {
    Character currentQuote = null;
    boolean escaping = false;
    int innerBrackets = 0;
    char[] chars = iFieldName.toCharArray();
    for (int i = nextSeparatorPos + 1; i < chars.length; i++) {
      char next = chars[i];
      if (escaping) {
        escaping = false;
      } else if (next == '\\') {
        escaping = true;
      } else if (next == '`' || next == '\'' || next == '"') {
        if (currentQuote == null) {
          currentQuote = next;
        } else if (currentQuote == next) {
          currentQuote = null;
        }

      } else if (next == '[') {
        innerBrackets++;
      } else if (next == ']') {
        if (innerBrackets == 0) {
          return i;
        }
        innerBrackets--;
      }
    }
    return -1;
  }

  private static boolean isFieldName(String indexAsString) {
    indexAsString = indexAsString.trim();
    if (indexAsString.startsWith("`") && indexAsString.endsWith("`")) {
      // quoted identifier
      return !indexAsString.substring(1, indexAsString.length() - 1).contains("`");
    }
    boolean firstChar = true;
    for (char c : indexAsString.toCharArray()) {
      if (isLetter(c) || (isNumber(c) && !firstChar)) {
        firstChar = false;
        continue;
      }
      return false;
    }
    return true;
  }

  private static boolean isNumber(char c) {
    return c >= '0' && c <= '9';
  }

  private static boolean isLetter(char c) {
    if (c == '$' || c == '_' || c == '@') {
      return true;
    }
    if (c >= 'a' && c <= 'z') {
      return true;
    }
    return c >= 'A' && c <= 'Z';
  }

  private static boolean isListOfNumbers(List<String> list) {
    for (String s : list) {
      try {
        Integer.parseInt(s);
      } catch (NumberFormatException ignore) {
        return false;
      }
    }
    return true;
  }

  protected static Object getIndexPart(final CommandContext iContext, final String indexPart) {
    Object index = indexPart;
    if (indexPart.indexOf(',') == -1
        && (indexPart.charAt(0) == '"' || indexPart.charAt(0) == '\'')) {
      index = OIOUtils.getStringContent(indexPart);
    } else if (indexPart.charAt(0) == '$') {
      final Object ctxValue = iContext.getVariable(indexPart);
      if (ctxValue == null) {
        return null;
      }
      index = ctxValue;
    } else if (!Character.isDigit(indexPart.charAt(0)))
    // GET FROM CURRENT VALUE
    {
      index = indexPart;
    }
    return index;
  }

  @SuppressWarnings("unchecked")
  protected static Object filterItem(
      YTDatabaseSessionInternal session, final String iConditionFieldName,
      final Object iConditionFieldValue, final Object iValue) {
    if (iValue instanceof YTIdentifiable) {
      final Record rec;
      try {
        rec = ((YTIdentifiable) iValue).getRecord();
      } catch (YTRecordNotFoundException rnf) {
        return null;
      }

      if (rec instanceof EntityImpl doc) {

        Object fieldValue = doc.field(iConditionFieldName);

        if (iConditionFieldValue == null) {
          return fieldValue == null ? doc : null;
        }

        fieldValue = YTType.convert(session, fieldValue, iConditionFieldValue.getClass());
        if (fieldValue != null && fieldValue.equals(iConditionFieldValue)) {
          return doc;
        }
      }
    } else if (iValue instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iValue;
      Object fieldValue = getMapEntry(session, map, iConditionFieldName);

      fieldValue = YTType.convert(session, fieldValue, iConditionFieldValue.getClass());
      if (fieldValue != null && fieldValue.equals(iConditionFieldValue)) {
        return map;
      }
    }
    return null;
  }

  /**
   * Retrieves the value crossing the map with the dotted notation
   *
   * @param session
   * @param iMap
   * @param iKey    Field(s) to retrieve. If are multiple fields, then the dot must be used as
   *                separator
   * @return
   */
  @SuppressWarnings("unchecked")
  public static Object getMapEntry(YTDatabaseSessionInternal session, final Map<String, ?> iMap,
      final Object iKey) {
    if (iMap == null || iKey == null) {
      return null;
    }

    if (iKey instanceof String iName) {
      int pos = iName.indexOf('.');
      if (pos > -1) {
        iName = iName.substring(0, pos);
      }

      final Object value = iMap.get(iName);
      if (value == null) {
        return null;
      }

      if (pos > -1) {
        final String restFieldName = iName.substring(pos + 1);
        if (value instanceof EntityImpl) {
          return getFieldValue(session, value, restFieldName);
        } else if (value instanceof Map<?, ?>) {
          return getMapEntry(session, (Map<String, ?>) value, restFieldName);
        }
      }

      return value;
    } else {
      return iMap.get(iKey);
    }
  }

  public static Object getIdentifiableValue(final YTIdentifiable iCurrent,
      final String iFieldName) {
    if (iFieldName == null) {
      return null;
    }

    if (!iFieldName.isEmpty()) {
      final char begin = iFieldName.charAt(0);
      if (begin == '@') {
        // RETURN AN ATTRIBUTE
        if (iFieldName.equalsIgnoreCase(ATTRIBUTE_THIS)) {
          return iCurrent.getRecord();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID)) {
          return iCurrent.getIdentity();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_ID)) {
          return iCurrent.getIdentity().getClusterId();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_POS)) {
          return iCurrent.getIdentity().getClusterPosition();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_VERSION)) {
          return iCurrent.getRecord().getVersion();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_CLASS)) {
          return ((EntityImpl) iCurrent.getRecord()).getClassName();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_TYPE)) {
          return YouTrackDBManager.instance()
              .getRecordFactoryManager()
              .getRecordTypeName(ORecordInternal.getRecordType(iCurrent.getRecord()));
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_SIZE)) {
          final byte[] stream = ((RecordAbstract) iCurrent.getRecord()).toStream();
          return stream != null ? stream.length : 0;
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_FIELDS)) {
          return ((EntityImpl) iCurrent.getRecord()).fieldNames();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RAW)) {
          return new String(((RecordAbstract) iCurrent.getRecord()).toStream());
        }
      }
    }
    if (iCurrent == null) {
      return null;
    }

    try {
      final EntityImpl doc = iCurrent.getRecord();
      return doc.accessProperty(iFieldName);
    } catch (YTRecordNotFoundException rnf) {
      return null;
    }
  }

  public static Object evaluateFunction(
      final Object currentValue, final String iFunction, final CommandContext iContext) {
    if (currentValue == null) {
      return null;
    }

    Object result = null;

    final String function = iFunction.toUpperCase(Locale.ENGLISH);

    if (function.startsWith("SIZE(")) {
      result = currentValue instanceof Record ? 1 : OMultiValue.getSize(currentValue);
    } else if (function.startsWith("LENGTH(")) {
      result = currentValue.toString().length();
    } else if (function.startsWith("TOUPPERCASE(")) {
      result = currentValue.toString().toUpperCase(Locale.ENGLISH);
    } else if (function.startsWith("TOLOWERCASE(")) {
      result = currentValue.toString().toLowerCase(Locale.ENGLISH);
    } else if (function.startsWith("TRIM(")) {
      result = currentValue.toString().trim();
    } else if (function.startsWith("TOJSON(")) {
      result = currentValue instanceof EntityImpl ? ((EntityImpl) currentValue).toJSON() : null;
    } else if (function.startsWith("KEYS(")) {
      result = currentValue instanceof Map<?, ?> ? ((Map<?, ?>) currentValue).keySet() : null;
    } else if (function.startsWith("VALUES(")) {
      result = currentValue instanceof Map<?, ?> ? ((Map<?, ?>) currentValue).values() : null;
    } else if (function.startsWith("ASSTRING(")) {
      result = currentValue.toString();
    } else if (function.startsWith("ASINTEGER(")) {
      result = Integer.parseInt(currentValue.toString());
    } else if (function.startsWith("ASFLOAT(")) {
      result = Float.parseFloat(currentValue.toString());
    } else if (function.startsWith("ASBOOLEAN(")) {
      if (currentValue instanceof String) {
        result = Boolean.parseBoolean((String) currentValue);
      } else if (currentValue instanceof Number) {
        final int bValue = ((Number) currentValue).intValue();
        if (bValue == 0) {
          result = Boolean.FALSE;
        } else if (bValue == 1) {
          result = Boolean.TRUE;
        }
      }
    } else if (function.startsWith("ASDATE(")) {
      if (currentValue instanceof Date) {
        result = currentValue;
      } else if (currentValue instanceof Number) {
        result = new Date(((Number) currentValue).longValue());
      } else {
        try {
          result =
              ODateHelper.getDateFormatInstance(ODatabaseRecordThreadLocal.instance().get())
                  .parse(currentValue.toString());
        } catch (ParseException ignore) {
        }
      }
    } else if (function.startsWith("ASDATETIME(")) {
      if (currentValue instanceof Date) {
        result = currentValue;
      } else if (currentValue instanceof Number) {
        result = new Date(((Number) currentValue).longValue());
      } else {
        try {
          result =
              ODateHelper.getDateTimeFormatInstance(ODatabaseRecordThreadLocal.instance().get())
                  .parse(currentValue.toString());
        } catch (ParseException ignore) {
        }
      }
    } else {
      // EXTRACT ARGUMENTS
      final List<String> args =
          OStringSerializerHelper.getParameters(iFunction.substring(iFunction.indexOf('(')));

      final Record currentRecord =
          iContext != null ? (Record) iContext.getVariable("$current") : null;
      for (int i = 0; i < args.size(); ++i) {
        final String arg = args.get(i);
        final Object o = OSQLHelper.getValue(arg, currentRecord, iContext);
        if (o != null) {
          args.set(i, o.toString());
        }
      }

      if (function.startsWith("CHARAT(")) {
        result = currentValue.toString().charAt(Integer.parseInt(args.get(0)));
      } else if (function.startsWith("INDEXOF(")) {
        if (args.size() == 1) {
          result = currentValue.toString().indexOf(OIOUtils.getStringContent(args.get(0)));
        } else {
          result =
              currentValue
                  .toString()
                  .indexOf(OIOUtils.getStringContent(args.get(0)), Integer.parseInt(args.get(1)));
        }
      } else if (function.startsWith("SUBSTRING(")) {
        if (args.size() == 1) {
          result = currentValue.toString().substring(Integer.parseInt(args.get(0)));
        } else {
          result =
              currentValue
                  .toString()
                  .substring(Integer.parseInt(args.get(0)), Integer.parseInt(args.get(1)));
        }
      } else if (function.startsWith("APPEND(")) {
        result = currentValue + OIOUtils.getStringContent(args.get(0));
      } else if (function.startsWith("PREFIX(")) {
        result = OIOUtils.getStringContent(args.get(0)) + currentValue;
      } else if (function.startsWith("FORMAT(")) {
        if (currentValue instanceof Date) {
          SimpleDateFormat formatter = new SimpleDateFormat(OIOUtils.getStringContent(args.get(0)));
          formatter.setTimeZone(ODateHelper.getDatabaseTimeZone());
          result = formatter.format(currentValue);
        } else {
          result = String.format(OIOUtils.getStringContent(args.get(0)), currentValue);
        }
      } else if (function.startsWith("LEFT(")) {
        final int len = Integer.parseInt(args.get(0));
        final String stringValue = currentValue.toString();
        result = stringValue.substring(0, len <= stringValue.length() ? len : stringValue.length());
      } else if (function.startsWith("RIGHT(")) {
        final int offset = Integer.parseInt(args.get(0));
        final String stringValue = currentValue.toString();
        result =
            stringValue.substring(
                offset < stringValue.length() ? stringValue.length() - offset : 0);
      } else {
        final OSQLFunctionRuntime f = OSQLHelper.getFunction(iContext.getDatabase(), null,
            iFunction);
        if (f != null) {
          result = f.execute(currentRecord, currentRecord, null, iContext);
        }
      }
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public static Object cloneValue(YTDatabaseSessionInternal db,
      EntityImpl iCloned, final Object fieldValue) {

    if (fieldValue != null) {
      if (fieldValue instanceof EntityImpl && ((EntityImpl) fieldValue).isEmbedded()) {
        // EMBEDDED DOCUMENT
        return ((EntityImpl) fieldValue).copy();
      } else if (fieldValue instanceof RidBag) {
        RidBag newBag = ((RidBag) fieldValue).copy(db);
        newBag.setOwner(null);
        newBag.setOwner(iCloned);
        return newBag;

      } else if (fieldValue instanceof LinkList) {
        return ((LinkList) fieldValue).copy(iCloned);

      } else if (fieldValue instanceof TrackedList<?>) {
        final TrackedList<Object> newList = new TrackedList<Object>(iCloned);
        newList.addAll((TrackedList<Object>) fieldValue);
        return newList;

      } else if (fieldValue instanceof List<?>) {
        return new ArrayList<>((List<Object>) fieldValue);
        // SETS
      } else if (fieldValue instanceof LinkSet) {
        final LinkSet newList = new LinkSet(iCloned);
        newList.addAll((LinkSet) fieldValue);
        return newList;

      } else if (fieldValue instanceof TrackedSet<?>) {
        final TrackedSet<Object> newList = new TrackedSet<Object>(iCloned);
        newList.addAll((TrackedSet<Object>) fieldValue);
        return newList;

      } else if (fieldValue instanceof Set<?>) {
        return new HashSet<Object>((Set<Object>) fieldValue);
        // MAPS
      } else if (fieldValue instanceof LinkMap) {
        final LinkMap newMap = new LinkMap(iCloned, ((LinkMap) fieldValue).getRecordType());
        newMap.putAll((LinkMap) fieldValue);
        return newMap;

      } else if (fieldValue instanceof TrackedMap) {
        final TrackedMap<Object> newMap = new TrackedMap<Object>(iCloned);
        newMap.putAll((TrackedMap<Object>) fieldValue);
        return newMap;

      } else if (fieldValue instanceof Map<?, ?>) {
        return new LinkedHashMap<String, Object>((Map<String, Object>) fieldValue);
      } else {
        return fieldValue;
      }
    }
    // else if (iCloned.getImmutableSchemaClass() != null) {
    // final YTProperty prop = iCloned.getImmutableSchemaClass().getProperty(iEntry.getKey());
    // if (prop != null && prop.isMandatory())
    // return fieldValue;
    // }
    return null;
  }

  public static boolean hasSameContentItem(
      final Object iCurrent,
      YTDatabaseSessionInternal iMyDb,
      final Object iOther,
      final YTDatabaseSessionInternal iOtherDb,
      RIDMapper ridMapper) {
    if (iCurrent instanceof EntityImpl current) {
      if (iOther instanceof YTRID) {
        if (!current.isDirty()) {
          YTRID id;
          if (ridMapper != null) {
            YTRID mappedId = ridMapper.map(current.getIdentity());
            if (mappedId != null) {
              id = mappedId;
            } else {
              id = current.getIdentity();
            }
          } else {
            id = current.getIdentity();
          }

          return id.equals(iOther);
        } else {
          final EntityImpl otherDoc = iOtherDb.load((YTRID) iOther);
          return ODocumentHelper.hasSameContentOf(current, iMyDb, otherDoc, iOtherDb, ridMapper);
        }
      } else {
        return ODocumentHelper.hasSameContentOf(
            current, iMyDb, (EntityImpl) iOther, iOtherDb, ridMapper);
      }
    } else {
      return compareScalarValues(iCurrent, iMyDb, iOther, iOtherDb, ridMapper);
    }
  }

  /**
   * Makes a deep comparison field by field to check if the passed EntityImpl instance is identical
   * as identity and content to the current one. Instead equals() just checks if the RID are the
   * same.
   *
   * @param iOther EntityImpl instance
   * @return true if the two document are identical, otherwise false
   * @see #equals(Object)
   */
  @SuppressWarnings("unchecked")
  public static boolean hasSameContentOf(
      final EntityImpl iCurrent,
      final YTDatabaseSessionInternal iMyDb,
      final EntityImpl iOther,
      final YTDatabaseSessionInternal iOtherDb,
      RIDMapper ridMapper) {
    return hasSameContentOf(iCurrent, iMyDb, iOther, iOtherDb, ridMapper, true);
  }

  /**
   * Makes a deep comparison field by field to check if the passed EntityImpl instance is identical
   * in the content to the current one. Instead equals() just checks if the RID are the same.
   *
   * @param iOther EntityImpl instance
   * @return true if the two document are identical, otherwise false
   * @see #equals(Object)
   */
  @SuppressWarnings("unchecked")
  public static boolean hasSameContentOf(
      final EntityImpl iCurrent,
      final YTDatabaseSessionInternal iMyDb,
      final EntityImpl iOther,
      final YTDatabaseSessionInternal iOtherDb,
      RIDMapper ridMapper,
      final boolean iCheckAlsoIdentity) {
    if (iOther == null) {
      return false;
    }

    if (iCheckAlsoIdentity
        && iCurrent.getIdentity().isValid()
        && !iCurrent.getIdentity().equals(iOther.getIdentity())) {
      return false;
    }

    if (iMyDb != null) {
      makeDbCall(
          iMyDb,
          new ODbRelatedCall<Object>() {
            public Object call(YTDatabaseSessionInternal database) {
              iCurrent.checkForFields();
              return null;
            }
          });
    } else {
      iCurrent.checkForFields();
    }

    if (iOtherDb != null) {
      makeDbCall(
          iOtherDb,
          new ODbRelatedCall<Object>() {
            public Object call(YTDatabaseSessionInternal database) {
              iOther.checkForFields();
              return null;
            }
          });
    } else {
      iOther.checkForFields();
    }

    if ((int) makeDbCall(iMyDb, database -> iCurrent.fields())
        != makeDbCall(iOtherDb, database -> iOther.fields())) {
      return false;
    }

    // CHECK FIELD-BY-FIELD
    Object myFieldValue;
    Object otherFieldValue;

    var propertyNames = makeDbCall(iMyDb, database -> iCurrent.getPropertyNames());
    for (var name : propertyNames) {
      myFieldValue = iCurrent.fields.get(name).value;
      otherFieldValue = iOther.fields.get(name).value;

      if (myFieldValue == otherFieldValue) {
        continue;
      }

      // CHECK FOR NULLS
      if (myFieldValue == null) {
        return false;
      } else if (otherFieldValue == null) {
        return false;
      }

      if (myFieldValue instanceof Set && otherFieldValue instanceof Set) {
        if (!compareSets(
            iMyDb, (Set<?>) myFieldValue, iOtherDb, (Set<?>) otherFieldValue, ridMapper)) {
          return false;
        }
      } else if (myFieldValue instanceof Collection && otherFieldValue instanceof Collection) {
        if (!compareCollections(
            iMyDb,
            (Collection<?>) myFieldValue,
            iOtherDb,
            (Collection<?>) otherFieldValue,
            ridMapper)) {
          return false;
        }
      } else if (myFieldValue instanceof RidBag && otherFieldValue instanceof RidBag) {
        if (!compareBags(
            iMyDb, (RidBag) myFieldValue, iOtherDb, (RidBag) otherFieldValue, ridMapper)) {
          return false;
        }
      } else if (myFieldValue instanceof Map && otherFieldValue instanceof Map) {
        if (!compareMaps(
            iMyDb,
            (Map<Object, Object>) myFieldValue,
            iOtherDb,
            (Map<Object, Object>) otherFieldValue,
            ridMapper)) {
          return false;
        }
      } else if (myFieldValue instanceof EntityImpl && otherFieldValue instanceof EntityImpl) {
        if (!hasSameContentOf(
            (EntityImpl) myFieldValue, iMyDb, (EntityImpl) otherFieldValue, iOtherDb,
            ridMapper)) {
          return false;
        }
      } else {
        if (!compareScalarValues(myFieldValue, iMyDb, otherFieldValue, iOtherDb, ridMapper)) {
          return false;
        }
      }
    }

    return true;
  }

  public static boolean compareMaps(
      YTDatabaseSessionInternal iMyDb,
      Map<Object, Object> myFieldValue,
      YTDatabaseSessionInternal iOtherDb,
      Map<Object, Object> otherFieldValue,
      RIDMapper ridMapper) {
    // CHECK IF THE ORDER IS RESPECTED
    final Map<Object, Object> myMap = myFieldValue;
    final Map<Object, Object> otherMap = otherFieldValue;

    if (myMap.size() != otherMap.size()) {
      return false;
    }

    boolean oldMyAutoConvert = false;
    boolean oldOtherAutoConvert = false;

    final Iterator<Entry<Object, Object>> myEntryIterator =
        makeDbCall(
            iMyDb,
            new ODbRelatedCall<Iterator<Entry<Object, Object>>>() {
              public Iterator<Entry<Object, Object>> call(YTDatabaseSessionInternal database) {
                return myMap.entrySet().iterator();
              }
            });

    while (makeDbCall(
        iMyDb,
        new ODbRelatedCall<Boolean>() {
          public Boolean call(YTDatabaseSessionInternal database) {
            return myEntryIterator.hasNext();
          }
        })) {
      final Entry<Object, Object> myEntry =
          makeDbCall(
              iMyDb,
              new ODbRelatedCall<Entry<Object, Object>>() {
                public Entry<Object, Object> call(YTDatabaseSessionInternal database) {
                  return myEntryIterator.next();
                }
              });
      final Object myKey =
          makeDbCall(
              iMyDb,
              new ODbRelatedCall<Object>() {
                public Object call(YTDatabaseSessionInternal database) {
                  return myEntry.getKey();
                }
              });

      if (makeDbCall(
          iOtherDb,
          new ODbRelatedCall<Boolean>() {
            public Boolean call(YTDatabaseSessionInternal database) {
              return !otherMap.containsKey(myKey);
            }
          })) {
        return false;
      }

      if (myEntry.getValue() instanceof EntityImpl) {
        if (!hasSameContentOf(
            makeDbCall(
                iMyDb,
                new ODbRelatedCall<EntityImpl>() {
                  public EntityImpl call(YTDatabaseSessionInternal database) {
                    return (EntityImpl) myEntry.getValue();
                  }
                }),
            iMyDb,
            makeDbCall(
                iOtherDb,
                new ODbRelatedCall<EntityImpl>() {
                  public EntityImpl call(YTDatabaseSessionInternal database) {
                    return ((YTIdentifiable) otherMap.get(myEntry.getKey())).getRecord();
                  }
                }),
            iOtherDb,
            ridMapper)) {
          return false;
        }
      } else {
        final Object myValue =
            makeDbCall(
                iMyDb,
                new ODbRelatedCall<Object>() {
                  public Object call(YTDatabaseSessionInternal database) {
                    return myEntry.getValue();
                  }
                });

        final Object otherValue =
            makeDbCall(
                iOtherDb,
                new ODbRelatedCall<Object>() {
                  public Object call(YTDatabaseSessionInternal database) {
                    return otherMap.get(myEntry.getKey());
                  }
                });

        if (!compareScalarValues(myValue, iMyDb, otherValue, iOtherDb, ridMapper)) {
          return false;
        }
      }
    }
    return true;
  }

  public static boolean compareCollections(
      YTDatabaseSessionInternal iMyDb,
      Collection<?> myFieldValue,
      YTDatabaseSessionInternal iOtherDb,
      Collection<?> otherFieldValue,
      RIDMapper ridMapper) {
    final Collection<?> myCollection = myFieldValue;
    final Collection<?> otherCollection = otherFieldValue;

    if (myCollection.size() != otherCollection.size()) {
      return false;
    }

    final Iterator<?> myIterator =
        makeDbCall(
            iMyDb,
            new ODbRelatedCall<Iterator<?>>() {
              public Iterator<?> call(YTDatabaseSessionInternal database) {
                return myCollection.iterator();
              }
            });

    final Iterator<?> otherIterator =
        makeDbCall(
            iOtherDb,
            new ODbRelatedCall<Iterator<?>>() {
              public Iterator<?> call(YTDatabaseSessionInternal database) {
                return otherCollection.iterator();
              }
            });

    while (makeDbCall(
        iMyDb,
        new ODbRelatedCall<Boolean>() {
          public Boolean call(YTDatabaseSessionInternal database) {
            return myIterator.hasNext();
          }
        })) {
      final Object myNextVal =
          makeDbCall(
              iMyDb,
              new ODbRelatedCall<Object>() {
                public Object call(YTDatabaseSessionInternal database) {
                  return myIterator.next();
                }
              });

      final Object otherNextVal =
          makeDbCall(
              iOtherDb,
              new ODbRelatedCall<Object>() {
                public Object call(YTDatabaseSessionInternal database) {
                  return otherIterator.next();
                }
              });

      if (!hasSameContentItem(myNextVal, iMyDb, otherNextVal, iOtherDb, ridMapper)) {
        return false;
      }
    }
    return true;
  }

  public static boolean compareSets(
      YTDatabaseSessionInternal iMyDb,
      Set<?> myFieldValue,
      YTDatabaseSessionInternal iOtherDb,
      Set<?> otherFieldValue,
      RIDMapper ridMapper) {
    final Set<?> mySet = myFieldValue;
    final Set<?> otherSet = otherFieldValue;

    final int mySize =
        makeDbCall(
            iMyDb,
            new ODbRelatedCall<Integer>() {
              public Integer call(YTDatabaseSessionInternal database) {
                return mySet.size();
              }
            });

    final int otherSize =
        makeDbCall(
            iOtherDb,
            new ODbRelatedCall<Integer>() {
              public Integer call(YTDatabaseSessionInternal database) {
                return otherSet.size();
              }
            });

    if (mySize != otherSize) {
      return false;
    }

    final Iterator<?> myIterator =
        makeDbCall(
            iMyDb,
            new ODbRelatedCall<Iterator<?>>() {
              public Iterator<?> call(YTDatabaseSessionInternal database) {
                return mySet.iterator();
              }
            });

    while (makeDbCall(
        iMyDb,
        new ODbRelatedCall<Boolean>() {
          public Boolean call(YTDatabaseSessionInternal database) {
            return myIterator.hasNext();
          }
        })) {

      final Iterator<?> otherIterator =
          makeDbCall(
              iOtherDb,
              new ODbRelatedCall<Iterator<?>>() {
                public Iterator<?> call(YTDatabaseSessionInternal database) {
                  return otherSet.iterator();
                }
              });

      final Object myNextVal =
          makeDbCall(
              iMyDb,
              new ODbRelatedCall<Object>() {
                public Object call(YTDatabaseSessionInternal database) {
                  return myIterator.next();
                }
              });

      boolean found = false;
      while (!found
          && makeDbCall(
          iOtherDb,
          new ODbRelatedCall<Boolean>() {
            public Boolean call(YTDatabaseSessionInternal database) {
              return otherIterator.hasNext();
            }
          })) {
        final Object otherNextVal =
            makeDbCall(
                iOtherDb,
                new ODbRelatedCall<Object>() {
                  public Object call(YTDatabaseSessionInternal database) {
                    return otherIterator.next();
                  }
                });

        found = hasSameContentItem(myNextVal, iMyDb, otherNextVal, iOtherDb, ridMapper);
      }

      if (!found) {
        return false;
      }
    }
    return true;
  }

  public static boolean compareBags(
      YTDatabaseSessionInternal iMyDb,
      RidBag myFieldValue,
      YTDatabaseSessionInternal iOtherDb,
      RidBag otherFieldValue,
      RIDMapper ridMapper) {
    final RidBag myBag = myFieldValue;
    final RidBag otherBag = otherFieldValue;

    final YTDatabaseSessionInternal currentDb = ODatabaseRecordThreadLocal.instance()
        .getIfDefined();
    try {

      final int mySize =
          makeDbCall(
              iMyDb,
              new ODbRelatedCall<Integer>() {
                public Integer call(YTDatabaseSessionInternal database) {
                  return myBag.size();
                }
              });

      final int otherSize =
          makeDbCall(
              iOtherDb,
              new ODbRelatedCall<Integer>() {
                public Integer call(YTDatabaseSessionInternal database) {
                  return otherBag.size();
                }
              });

      if (mySize != otherSize) {
        return false;
      }

      final List<YTRID> otherBagCopy =
          makeDbCall(
              iOtherDb,
              new ODbRelatedCall<List<YTRID>>() {
                @Override
                public List<YTRID> call(final YTDatabaseSessionInternal database) {
                  final List<YTRID> otherRidBag = new LinkedList<YTRID>();
                  for (YTIdentifiable identifiable : otherBag) {
                    otherRidBag.add(identifiable.getIdentity());
                  }

                  return otherRidBag;
                }
              });

      final Iterator<YTIdentifiable> myIterator =
          makeDbCall(
              iMyDb,
              new ODbRelatedCall<Iterator<YTIdentifiable>>() {
                public Iterator<YTIdentifiable> call(final YTDatabaseSessionInternal database) {
                  return myBag.iterator();
                }
              });

      while (makeDbCall(
          iMyDb,
          new ODbRelatedCall<Boolean>() {
            public Boolean call(final YTDatabaseSessionInternal database) {
              return myIterator.hasNext();
            }
          })) {
        final YTIdentifiable myIdentifiable =
            makeDbCall(
                iMyDb,
                new ODbRelatedCall<YTIdentifiable>() {
                  @Override
                  public YTIdentifiable call(final YTDatabaseSessionInternal database) {
                    return myIterator.next();
                  }
                });

        final YTRID otherRid;
        if (ridMapper != null) {
          YTRID convertedRid = ridMapper.map(myIdentifiable.getIdentity());
          if (convertedRid != null) {
            otherRid = convertedRid;
          } else {
            otherRid = myIdentifiable.getIdentity();
          }
        } else {
          otherRid = myIdentifiable.getIdentity();
        }

        makeDbCall(
            iOtherDb,
            new ODbRelatedCall<Object>() {
              @Override
              public Object call(final YTDatabaseSessionInternal database) {
                otherBagCopy.remove(otherRid);
                return null;
              }
            });
      }

      return makeDbCall(
          iOtherDb,
          new ODbRelatedCall<Boolean>() {
            @Override
            public Boolean call(YTDatabaseSessionInternal database) {
              return otherBagCopy.isEmpty();
            }
          });
    } finally {
      if (currentDb != null) {
        currentDb.activateOnCurrentThread();
      }
    }
  }

  private static boolean compareScalarValues(
      Object myValue,
      YTDatabaseSessionInternal iMyDb,
      Object otherValue,
      YTDatabaseSessionInternal iOtherDb,
      RIDMapper ridMapper) {
    if (myValue == null && otherValue != null || myValue != null && otherValue == null) {
      return false;
    }

    if (myValue == null) {
      return true;
    }

    if (myValue.getClass().isArray() && !otherValue.getClass().isArray()
        || !myValue.getClass().isArray() && otherValue.getClass().isArray()) {
      return false;
    }

    if (myValue.getClass().isArray() && otherValue.getClass().isArray()) {
      final int myArraySize = Array.getLength(myValue);
      final int otherArraySize = Array.getLength(otherValue);

      if (myArraySize != otherArraySize) {
        return false;
      }

      for (int i = 0; i < myArraySize; i++) {
        final Object first = Array.get(myValue, i);
        final Object second = Array.get(otherValue, i);
        if (first == null && second != null) {
          return false;
        }
        if (first instanceof EntityImpl && second instanceof EntityImpl) {
          return hasSameContentOf(
              (EntityImpl) first, iMyDb, (EntityImpl) second, iOtherDb, ridMapper);
        }

        if (first != null && !first.equals(second)) {
          return false;
        }
      }

      return true;
    }

    if (myValue instanceof Number myNumberValue && otherValue instanceof Number otherNumberValue) {

      if (isInteger(myNumberValue) && isInteger(otherNumberValue)) {
        return myNumberValue.longValue() == otherNumberValue.longValue();
      } else if (isFloat(myNumberValue) && isFloat(otherNumberValue)) {
        return myNumberValue.doubleValue() == otherNumberValue.doubleValue();
      }
    }

    if (ridMapper != null
        && myValue instanceof YTIdentifiable myIdentifiableValue
        && otherValue instanceof YTIdentifiable otherIdentifiableValue) {
      myValue = myIdentifiableValue.getIdentity();
      otherValue = otherIdentifiableValue.getIdentity();
      if (((YTRID) myValue).isPersistent()) {
        YTRID convertedValue = ridMapper.map((YTRID) myValue);
        if (convertedValue != null) {
          myValue = convertedValue;
        }
      }
    }

    if (myValue instanceof Date && otherValue instanceof Date) {
      return ((Date) myValue).getTime() / 1000 == ((Date) otherValue).getTime() / 1000;
    }

    return myValue.equals(otherValue);
  }

  private static boolean isInteger(Number value) {
    return value instanceof Byte
        || value instanceof Short
        || value instanceof Integer
        || value instanceof Long;
  }

  private static boolean isFloat(Number value) {
    return value instanceof Float || value instanceof Double;
  }

  public static <T> T makeDbCall(
      final YTDatabaseSessionInternal databaseRecord, final ODbRelatedCall<T> function) {
    databaseRecord.activateOnCurrentThread();
    return function.call(databaseRecord);
  }
}
