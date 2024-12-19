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

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.QueryParsingException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerStringAbstract;
import com.jetbrains.youtrack.db.internal.core.sql.SQLEngine;
import com.jetbrains.youtrack.db.internal.core.sql.SQLHelper;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLPredicate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionRuntime;
import com.jetbrains.youtrack.db.internal.core.sql.method.SQLMethod;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
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
public class EntityHelper {

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

  public interface DbRelatedCall<T> {

    T call(DatabaseSessionInternal database);
  }

  public interface RIDMapper {

    RID map(RID rid);
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
      List<? extends Identifiable> ioResultSet,
      List<Pair<String, String>> iOrderCriteria,
      CommandContext context) {
    if (ioResultSet != null) {
      ioResultSet.sort(new EntityComparator(iOrderCriteria, context));
    }
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET convertField(
      DatabaseSessionInternal session, final EntityImpl entity,
      final String iFieldName,
      PropertyType type,
      Class<?> iFieldType,
      Object iValue) {
    if (iFieldType == null && type != null) {
      iFieldType = type.getDefaultJavaType();
    }

    if (iFieldType == null) {
      return (RET) iValue;
    }

    if (RID.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof RID) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new RecordId((String) iValue);
      } else if (iValue instanceof Record) {
        return (RET) ((Record) iValue).getIdentity();
      }
    } else if (Identifiable.class.isAssignableFrom(iFieldType)) {
      if (iValue instanceof RID || iValue instanceof Record) {
        return (RET) iValue;
      } else if (iValue instanceof String) {
        return (RET) new RecordId((String) iValue);
      } else if (MultiValue.isMultiValue(iValue) && MultiValue.getSize(iValue) == 1) {
        Object val = MultiValue.getFirstValue(iValue);
        if (val instanceof Result) {
          val = ((Result) val).getIdentity().orElse(null);
        }
        if (val instanceof Identifiable) {
          return (RET) val;
        }
      }
    } else if (Set.class.isAssignableFrom(iFieldType)) {
      if (!(iValue instanceof Set)) {
        // CONVERT IT TO SET
        final Collection<?> newValue;
        if (type.isLink()) {
          newValue = new LinkSet(entity);
        } else {
          newValue = new TrackedSet<>(entity);
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
        } else if (MultiValue.isMultiValue(iValue)) {
          // GENERIC MULTI VALUE
          for (Object s : MultiValue.getMultiValueIterable(iValue)) {
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
          newValue = new LinkList(entity);
        } else {
          newValue = new TrackedList<Object>(entity);
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
        } else if (MultiValue.isMultiValue(iValue)) {
          // GENERIC MULTI VALUE
          for (Object s : MultiValue.getMultiValueIterable(iValue)) {
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
      if (iValue instanceof String && DatabaseRecordThreadLocal.instance().isDefined()) {
        DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().get();
        final StorageConfiguration config = db.getStorageInfo().getConfiguration();

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
          throw BaseException.wrapException(
              new QueryParsingException(
                  "Error on conversion of date '" + iValue + "' using the format: " + dateFormat),
              pe);
        }
      }
    }

    iValue = PropertyType.convert(session, iValue, iFieldType);

    return (RET) iValue;
  }

  public static <RET> RET getFieldValue(DatabaseSessionInternal db, Object value,
      final String iFieldName) {
    var context = new BasicCommandContext();
    context.setDatabase(db);

    return getFieldValue(db, value, iFieldName, context);
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET getFieldValue(
      DatabaseSessionInternal db, Object value, final String iFieldName,
      @Nonnull final CommandContext iContext) {
    if (value == null) {
      return null;
    }

    final int fieldNameLength = iFieldName.length();
    if (fieldNameLength == 0) {
      return (RET) value;
    }

    Identifiable currentRecord = value instanceof Identifiable ? (Identifiable) value : null;

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
            value = getIdentifiableValue(db, currentRecord, fieldName);
          } else if (value instanceof Map<?, ?>) {
            value = getMapEntry(db, (Map<String, ?>) value, fieldName);
          } else if (MultiValue.isMultiValue(value)) {
            final HashSet<Object> temp = new LinkedHashSet<Object>();
            for (Object o : MultiValue.getMultiValueIterable(value)) {
              if (o instanceof Identifiable) {
                Object r = getFieldValue(db, o, iFieldName);
                if (r != null) {
                  MultiValue.add(temp, r);
                }
              }
            }
            value = temp;
          }
        }

        if (value == null) {
          return null;
        } else if (value instanceof Identifiable) {
          currentRecord = (Identifiable) value;
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

        if (value instanceof Identifiable) {
          final Record record =
              currentRecord instanceof Identifiable ? currentRecord.getRecord(db) : null;

          final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts =
              StringSerializerHelper.smartSplit(
                  indexAsString, ',', StringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final List<String> indexRanges =
              StringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final List<String> indexCondition =
              StringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
          // SINGLE VALUE
          {
            value = ((EntityImpl) record).field(indexAsString);
          } else if (indexParts.size() > 1) {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = ((EntityImpl) record).field(
                  IOUtils.getStringContent(indexParts.get(i)));
            }
            value = values;
          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            String from = indexRanges.get(0);
            String to = indexRanges.get(1);

            final EntityImpl entity = (EntityImpl) record;

            final String[] fieldNames = entity.fieldNames();
            final int rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo =
                to != null && !to.isEmpty()
                    ? Math.min(Integer.parseInt(to), fieldNames.length - 1)
                    : fieldNames.length - 1;

            final Object[] values = new Object[rangeTo - rangeFrom + 1];

            for (int i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = entity.field(fieldNames[i]);
            }

            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue =
                RecordSerializerStringAbstract.getTypeValue(db, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = IOUtils.getStringContent(conditionFieldValue);
            }

            final Object fieldValue = getFieldValue(db, currentRecord, conditionFieldName);

            if (conditionFieldValue != null && fieldValue != null) {
              conditionFieldValue = PropertyType.convert(db, conditionFieldValue,
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
              StringSerializerHelper.smartSplit(
                  indexAsString, ',', StringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final List<String> indexRanges =
              StringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final List<String> indexCondition =
              StringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          final Map<String, ?> map = (Map<String, ?>) value;
          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
          // SINGLE VALUE
          {
            value = map.get(index);
          } else if (indexParts.size() > 1) {
            // MULTI VALUE
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = map.get(IOUtils.getStringContent(indexParts.get(i)));
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
                RecordSerializerStringAbstract.getTypeValue(db, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = IOUtils.getStringContent(conditionFieldValue);
            }

            final Object fieldValue = map.get(conditionFieldName);

            if (conditionFieldValue != null && fieldValue != null) {
              conditionFieldValue = PropertyType.convert(db, conditionFieldValue,
                  fieldValue.getClass());
            }

            if (fieldValue == null && !conditionFieldValue.equals("null")
                || fieldValue != null && !fieldValue.equals(conditionFieldValue)) {
              value = null;
            }
          }

        } else if (MultiValue.isMultiValue(value)) {
          // MULTI VALUE
          final Object index = getIndexPart(iContext, indexPart);
          final String indexAsString = index != null ? index.toString() : null;

          final List<String> indexParts = StringSerializerHelper.smartSplit(indexAsString, ',');
          final List<String> indexRanges = StringSerializerHelper.smartSplit(indexAsString, '-');
          final List<String> indexCondition =
              StringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          if (isFieldName(indexAsString)) {
            // SINGLE VALUE
            if (value instanceof Map<?, ?>) {
              value = getMapEntry(db, (Map<String, ?>) value, index);
            } else if (Character.isDigit(indexAsString.charAt(0))) {
              value = MultiValue.getValue(value, Integer.parseInt(indexAsString));
            } else
            // FILTER BY FIELD
            {
              value = getFieldValue(db, value, indexAsString, iContext);
            }

          } else if (isListOfNumbers(indexParts)) {

            // MULTI VALUES
            final Object[] values = new Object[indexParts.size()];
            for (int i = 0; i < indexParts.size(); ++i) {
              values[i] = MultiValue.getValue(value, Integer.parseInt(indexParts.get(i)));
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
              rangeTo = Math.min(Integer.parseInt(to), MultiValue.getSize(value) - 1);
            } else {
              rangeTo = MultiValue.getSize(value) - 1;
            }

            int arraySize = rangeTo - rangeFrom + 1;
            if (arraySize < 0) {
              arraySize = 0;
            }
            final Object[] values = new Object[arraySize];
            for (int i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = MultiValue.getValue(value, i);
            }
            value = values;

          } else {
            // CONDITION
            SQLPredicate pred = new SQLPredicate(iContext, indexAsString);
            final HashSet<Object> values = new LinkedHashSet<Object>();

            for (Object v : MultiValue.getMultiValueIterable(value)) {
              if (v instanceof Identifiable) {
                Object result =
                    pred.evaluate((Identifiable) v, ((Identifiable) v).getRecord(db), iContext);
                if (Boolean.TRUE.equals(result)) {
                  values.add(v);
                }
              } else if (v instanceof Map) {
                EntityImpl entity = new EntityImpl(db);
                entity.fromMap((Map<String, ? extends Object>) v);
                Object result = pred.evaluate(entity, entity, iContext);
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
            SQLMethod method =
                SQLEngine.getMethod(fieldName.substring(0, fieldName.length() - 2));
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
              StringSerializerHelper.smartSplit(fieldName, '=', ' ');

          if (indexCondition.size() == 2) {
            final String conditionFieldName = indexCondition.get(0);
            Object conditionFieldValue =
                RecordSerializerStringAbstract.getTypeValue(db, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = IOUtils.getStringContent(conditionFieldValue);
            }

            value = filterItem(db, conditionFieldName, conditionFieldValue, value);

          } else if (currentRecord != null) {
            // GET THE LINKED OBJECT IF ANY
            value = getIdentifiableValue(db, currentRecord, fieldName);
          } else if (value instanceof Map<?, ?>) {
            value = getMapEntry(db, (Map<String, ?>) value, fieldName);
          } else if (MultiValue.isMultiValue(value)) {
            final Set<Object> values = new LinkedHashSet<Object>();
            for (Object v : MultiValue.getMultiValueIterable(value)) {
              final Object item;

              if (v instanceof Identifiable) {
                item = getIdentifiableValue(db, (Identifiable) v, fieldName);
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

      if (value instanceof Identifiable) {
        currentRecord = (Identifiable) value;
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
      index = IOUtils.getStringContent(indexPart);
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
      DatabaseSessionInternal db, final String iConditionFieldName,
      final Object iConditionFieldValue, final Object iValue) {
    if (iValue instanceof Identifiable) {
      final Record rec;
      try {
        rec = ((Identifiable) iValue).getRecord(db);
      } catch (RecordNotFoundException rnf) {
        return null;
      }

      if (rec instanceof EntityImpl entity) {

        Object fieldValue = entity.field(iConditionFieldName);

        if (iConditionFieldValue == null) {
          return fieldValue == null ? entity : null;
        }

        fieldValue = PropertyType.convert(db, fieldValue, iConditionFieldValue.getClass());
        if (fieldValue != null && fieldValue.equals(iConditionFieldValue)) {
          return entity;
        }
      }
    } else if (iValue instanceof Map<?, ?>) {
      final Map<String, ?> map = (Map<String, ?>) iValue;
      Object fieldValue = getMapEntry(db, map, iConditionFieldName);

      fieldValue = PropertyType.convert(db, fieldValue, iConditionFieldValue.getClass());
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
  public static Object getMapEntry(DatabaseSessionInternal session, final Map<String, ?> iMap,
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

  public static Object getIdentifiableValue(DatabaseSessionInternal db, final Identifiable iCurrent,
      final String iFieldName) {
    if (iFieldName == null) {
      return null;
    }

    if (!iFieldName.isEmpty()) {
      final char begin = iFieldName.charAt(0);
      if (begin == '@') {
        if (db == null) {
          throw new IllegalStateException(
              "Custom attribute can not be processed because record is not bound to the session");
        }

        assert db.assertIfNotActive();
        // RETURN AN ATTRIBUTE
        if (iFieldName.equalsIgnoreCase(ATTRIBUTE_THIS)) {
          return iCurrent.getRecord(db);
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID)) {
          return iCurrent.getIdentity();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_ID)) {
          return iCurrent.getIdentity().getClusterId();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_POS)) {
          return iCurrent.getIdentity().getClusterPosition();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_VERSION)) {
          return iCurrent.getRecord(db).getVersion();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_CLASS)) {
          return ((EntityImpl) iCurrent.getRecord(db)).getClassName();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_TYPE)) {
          return YouTrackDBEnginesManager.instance()
              .getRecordFactoryManager()
              .getRecordTypeName(RecordInternal.getRecordType(db, iCurrent.getRecord(db)));
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_SIZE)) {
          final byte[] stream = ((RecordAbstract) iCurrent.getRecord(db)).toStream();
          return stream != null ? stream.length : 0;
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_FIELDS)) {
          return ((EntityImpl) iCurrent.getRecord(db)).fieldNames();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RAW)) {
          return new String(((RecordAbstract) iCurrent.getRecord(db)).toStream());
        }
      }
    }
    if (iCurrent == null) {
      return null;
    }

    try {
      final EntityImpl entity = iCurrent.getRecord(db);
      return entity.accessProperty(iFieldName);
    } catch (RecordNotFoundException rnf) {
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
      result = currentValue instanceof Record ? 1 : MultiValue.getSize(currentValue);
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
              DateHelper.getDateFormatInstance(DatabaseRecordThreadLocal.instance().get())
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
              DateHelper.getDateTimeFormatInstance(DatabaseRecordThreadLocal.instance().get())
                  .parse(currentValue.toString());
        } catch (ParseException ignore) {
        }
      }
    } else {
      // EXTRACT ARGUMENTS
      final List<String> args =
          StringSerializerHelper.getParameters(iFunction.substring(iFunction.indexOf('(')));

      final Record currentRecord =
          iContext != null ? (Record) iContext.getVariable("$current") : null;
      for (int i = 0; i < args.size(); ++i) {
        final String arg = args.get(i);
        final Object o = SQLHelper.getValue(arg, currentRecord, iContext);
        if (o != null) {
          args.set(i, o.toString());
        }
      }

      if (function.startsWith("CHARAT(")) {
        result = currentValue.toString().charAt(Integer.parseInt(args.getFirst()));
      } else if (function.startsWith("INDEXOF(")) {
        if (args.size() == 1) {
          result = currentValue.toString().indexOf(IOUtils.getStringContent(args.getFirst()));
        } else {
          result =
              currentValue
                  .toString()
                  .indexOf(IOUtils.getStringContent(args.get(0)), Integer.parseInt(args.get(1)));
        }
      } else if (function.startsWith("SUBSTRING(")) {
        if (args.size() == 1) {
          result = currentValue.toString().substring(Integer.parseInt(args.getFirst()));
        } else {
          result =
              currentValue
                  .toString()
                  .substring(Integer.parseInt(args.get(0)), Integer.parseInt(args.get(1)));
        }
      } else if (function.startsWith("APPEND(")) {
        result = currentValue + IOUtils.getStringContent(args.getFirst());
      } else if (function.startsWith("PREFIX(")) {
        result = IOUtils.getStringContent(args.getFirst()) + currentValue;
      } else if (function.startsWith("FORMAT(")) {
        if (currentValue instanceof Date) {
          SimpleDateFormat formatter = new SimpleDateFormat(
              IOUtils.getStringContent(args.getFirst()));
          formatter.setTimeZone(DateHelper.getDatabaseTimeZone());
          result = formatter.format(currentValue);
        } else {
          result = String.format(IOUtils.getStringContent(args.getFirst()), currentValue);
        }
      } else if (function.startsWith("LEFT(")) {
        final int len = Integer.parseInt(args.getFirst());
        final String stringValue = currentValue.toString();
        result = stringValue.substring(0, Math.min(len, stringValue.length()));
      } else if (function.startsWith("RIGHT(")) {
        final int offset = Integer.parseInt(args.getFirst());
        final String stringValue = currentValue.toString();
        result =
            stringValue.substring(
                offset < stringValue.length() ? stringValue.length() - offset : 0);
      } else {
        final SQLFunctionRuntime f = SQLHelper.getFunction(iContext.getDatabase(), null,
            iFunction);
        if (f != null) {
          result = f.execute(currentRecord, currentRecord, null, iContext);
        }
      }
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public static Object cloneValue(DatabaseSessionInternal db,
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
    // final Property prop = iCloned.getImmutableSchemaClass().getProperty(iEntry.getKey());
    // if (prop != null && prop.isMandatory())
    // return fieldValue;
    // }
    return null;
  }

  public static boolean hasSameContentItem(
      final Object iCurrent,
      DatabaseSessionInternal iMyDb,
      final Object iOther,
      final DatabaseSessionInternal iOtherDb,
      RIDMapper ridMapper) {
    if (iCurrent instanceof EntityImpl current) {
      if (iOther instanceof RID) {
        if (!current.isDirty()) {
          RID id;
          if (ridMapper != null) {
            RID mappedId = ridMapper.map(current.getIdentity());
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
          final EntityImpl otherEntity = iOtherDb.load((RID) iOther);
          return EntityHelper.hasSameContentOf(current, iMyDb, otherEntity, iOtherDb, ridMapper);
        }
      } else {
        return EntityHelper.hasSameContentOf(
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
      final DatabaseSessionInternal iMyDb,
      final EntityImpl iOther,
      final DatabaseSessionInternal iOtherDb,
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
      final DatabaseSessionInternal iMyDb,
      final EntityImpl iOther,
      final DatabaseSessionInternal iOtherDb,
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
          new DbRelatedCall<Object>() {
            public Object call(DatabaseSessionInternal database) {
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
          new DbRelatedCall<Object>() {
            public Object call(DatabaseSessionInternal database) {
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
      DatabaseSessionInternal iMyDb,
      Map<Object, Object> myFieldValue,
      DatabaseSessionInternal iOtherDb,
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
            new DbRelatedCall<Iterator<Entry<Object, Object>>>() {
              public Iterator<Entry<Object, Object>> call(DatabaseSessionInternal database) {
                return myMap.entrySet().iterator();
              }
            });

    while (makeDbCall(
        iMyDb,
        new DbRelatedCall<Boolean>() {
          public Boolean call(DatabaseSessionInternal database) {
            return myEntryIterator.hasNext();
          }
        })) {
      final Entry<Object, Object> myEntry =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Entry<Object, Object>>() {
                public Entry<Object, Object> call(DatabaseSessionInternal database) {
                  return myEntryIterator.next();
                }
              });
      final Object myKey =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Object>() {
                public Object call(DatabaseSessionInternal database) {
                  return myEntry.getKey();
                }
              });

      if (makeDbCall(
          iOtherDb,
          new DbRelatedCall<Boolean>() {
            public Boolean call(DatabaseSessionInternal database) {
              return !otherMap.containsKey(myKey);
            }
          })) {
        return false;
      }

      if (myEntry.getValue() instanceof EntityImpl) {
        if (!hasSameContentOf(
            makeDbCall(
                iMyDb,
                new DbRelatedCall<EntityImpl>() {
                  public EntityImpl call(DatabaseSessionInternal database) {
                    return (EntityImpl) myEntry.getValue();
                  }
                }),
            iMyDb,
            makeDbCall(
                iOtherDb,
                new DbRelatedCall<EntityImpl>() {
                  public EntityImpl call(DatabaseSessionInternal database) {
                    return ((Identifiable) otherMap.get(myEntry.getKey())).getRecord(database);
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
                new DbRelatedCall<Object>() {
                  public Object call(DatabaseSessionInternal database) {
                    return myEntry.getValue();
                  }
                });

        final Object otherValue =
            makeDbCall(
                iOtherDb,
                new DbRelatedCall<Object>() {
                  public Object call(DatabaseSessionInternal database) {
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
      DatabaseSessionInternal iMyDb,
      Collection<?> myFieldValue,
      DatabaseSessionInternal iOtherDb,
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
            new DbRelatedCall<Iterator<?>>() {
              public Iterator<?> call(DatabaseSessionInternal database) {
                return myCollection.iterator();
              }
            });

    final Iterator<?> otherIterator =
        makeDbCall(
            iOtherDb,
            new DbRelatedCall<Iterator<?>>() {
              public Iterator<?> call(DatabaseSessionInternal database) {
                return otherCollection.iterator();
              }
            });

    while (makeDbCall(
        iMyDb,
        new DbRelatedCall<Boolean>() {
          public Boolean call(DatabaseSessionInternal database) {
            return myIterator.hasNext();
          }
        })) {
      final Object myNextVal =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Object>() {
                public Object call(DatabaseSessionInternal database) {
                  return myIterator.next();
                }
              });

      final Object otherNextVal =
          makeDbCall(
              iOtherDb,
              new DbRelatedCall<Object>() {
                public Object call(DatabaseSessionInternal database) {
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
      DatabaseSessionInternal iMyDb,
      Set<?> myFieldValue,
      DatabaseSessionInternal iOtherDb,
      Set<?> otherFieldValue,
      RIDMapper ridMapper) {
    final Set<?> mySet = myFieldValue;
    final Set<?> otherSet = otherFieldValue;

    final int mySize =
        makeDbCall(
            iMyDb,
            new DbRelatedCall<Integer>() {
              public Integer call(DatabaseSessionInternal database) {
                return mySet.size();
              }
            });

    final int otherSize =
        makeDbCall(
            iOtherDb,
            new DbRelatedCall<Integer>() {
              public Integer call(DatabaseSessionInternal database) {
                return otherSet.size();
              }
            });

    if (mySize != otherSize) {
      return false;
    }

    final Iterator<?> myIterator =
        makeDbCall(
            iMyDb,
            new DbRelatedCall<Iterator<?>>() {
              public Iterator<?> call(DatabaseSessionInternal database) {
                return mySet.iterator();
              }
            });

    while (makeDbCall(
        iMyDb,
        new DbRelatedCall<Boolean>() {
          public Boolean call(DatabaseSessionInternal database) {
            return myIterator.hasNext();
          }
        })) {

      final Iterator<?> otherIterator =
          makeDbCall(
              iOtherDb,
              new DbRelatedCall<Iterator<?>>() {
                public Iterator<?> call(DatabaseSessionInternal database) {
                  return otherSet.iterator();
                }
              });

      final Object myNextVal =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Object>() {
                public Object call(DatabaseSessionInternal database) {
                  return myIterator.next();
                }
              });

      boolean found = false;
      while (!found
          && makeDbCall(
          iOtherDb,
          new DbRelatedCall<Boolean>() {
            public Boolean call(DatabaseSessionInternal database) {
              return otherIterator.hasNext();
            }
          })) {
        final Object otherNextVal =
            makeDbCall(
                iOtherDb,
                new DbRelatedCall<Object>() {
                  public Object call(DatabaseSessionInternal database) {
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
      DatabaseSessionInternal iMyDb,
      RidBag myFieldValue,
      DatabaseSessionInternal iOtherDb,
      RidBag otherFieldValue,
      RIDMapper ridMapper) {
    final RidBag myBag = myFieldValue;
    final RidBag otherBag = otherFieldValue;

    final DatabaseSessionInternal currentDb = DatabaseRecordThreadLocal.instance()
        .getIfDefined();
    try {

      final int mySize =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Integer>() {
                public Integer call(DatabaseSessionInternal database) {
                  return myBag.size();
                }
              });

      final int otherSize =
          makeDbCall(
              iOtherDb,
              new DbRelatedCall<Integer>() {
                public Integer call(DatabaseSessionInternal database) {
                  return otherBag.size();
                }
              });

      if (mySize != otherSize) {
        return false;
      }

      final List<RID> otherBagCopy =
          makeDbCall(
              iOtherDb,
              new DbRelatedCall<List<RID>>() {
                @Override
                public List<RID> call(final DatabaseSessionInternal database) {
                  final List<RID> otherRidBag = new LinkedList<RID>();
                  for (Identifiable identifiable : otherBag) {
                    otherRidBag.add(identifiable.getIdentity());
                  }

                  return otherRidBag;
                }
              });

      final Iterator<Identifiable> myIterator =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Iterator<Identifiable>>() {
                public Iterator<Identifiable> call(final DatabaseSessionInternal database) {
                  return myBag.iterator();
                }
              });

      while (makeDbCall(
          iMyDb,
          new DbRelatedCall<Boolean>() {
            public Boolean call(final DatabaseSessionInternal database) {
              return myIterator.hasNext();
            }
          })) {
        final Identifiable myIdentifiable =
            makeDbCall(
                iMyDb,
                new DbRelatedCall<Identifiable>() {
                  @Override
                  public Identifiable call(final DatabaseSessionInternal database) {
                    return myIterator.next();
                  }
                });

        final RID otherRid;
        if (ridMapper != null) {
          RID convertedRid = ridMapper.map(myIdentifiable.getIdentity());
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
            new DbRelatedCall<Object>() {
              @Override
              public Object call(final DatabaseSessionInternal database) {
                otherBagCopy.remove(otherRid);
                return null;
              }
            });
      }

      return makeDbCall(
          iOtherDb,
          new DbRelatedCall<Boolean>() {
            @Override
            public Boolean call(DatabaseSessionInternal database) {
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
      DatabaseSessionInternal iMyDb,
      Object otherValue,
      DatabaseSessionInternal iOtherDb,
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
        && myValue instanceof Identifiable myIdentifiableValue
        && otherValue instanceof Identifiable otherIdentifiableValue) {
      myValue = myIdentifiableValue.getIdentity();
      otherValue = otherIdentifiableValue.getIdentity();
      if (((RID) myValue).isPersistent()) {
        RID convertedValue = ridMapper.map((RID) myValue);
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
      final DatabaseSessionInternal databaseRecord, final DbRelatedCall<T> function) {
    databaseRecord.activateOnCurrentThread();
    return function.call(databaseRecord);
  }
}
