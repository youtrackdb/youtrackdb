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
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
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
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
  public static final String ATTRIBUTE_INTERNAL_SCHEMA = "@internalSchema";
  public static final String ATTRIBUTE_INTERNAL_INDEX_MANAGER = "@internalIndexManager";
  public static final String ATTRIBUTE_TYPE = "@type";
  public static final String ATTRIBUTE_EMBEDDED = "@embedded";
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
      @Nonnull DatabaseSessionInternal session, @Nonnull final EntityImpl entity,
      @Nonnull final String fieldName,
      @Nullable PropertyType type,
      @Nullable Class<?> fieldJavaClass,
      @Nullable Object value) {
    if (value == null) {
      return null;
    }

    if (type == null) {
      type = PropertyType.getTypeByValue(value);
    }

    if (fieldJavaClass == null && type != null) {
      fieldJavaClass = type.getDefaultJavaType();
    }

    if (fieldJavaClass == null) {
      return (RET) value;
    }

    if (RID.class.isAssignableFrom(fieldJavaClass)) {
      if (value instanceof RID) {
        return (RET) value;
      } else if (value instanceof String) {
        return (RET) new RecordId((String) value);
      } else if (value instanceof DBRecord) {
        return (RET) ((DBRecord) value).getIdentity();
      }
    } else if (Identifiable.class.isAssignableFrom(fieldJavaClass)) {
      if (value instanceof RID || value instanceof DBRecord) {
        return (RET) value;
      } else if (value instanceof String) {
        return (RET) new RecordId((String) value);
      } else if (MultiValue.isMultiValue(value) && MultiValue.getSize(value) == 1) {
        var val = MultiValue.getFirstValue(value);
        if (val instanceof Result) {
          val = ((Result) val).getIdentity();
        }
        if (val instanceof Identifiable) {
          return (RET) val;
        }
      }
    } else if (Set.class.isAssignableFrom(fieldJavaClass)) {
      if (!(value instanceof Set) || type.isLink() && !(value instanceof LinkSet) || !type.isLink()
          && (value instanceof LinkSet)) {
        // CONVERT IT TO SET
        final Collection<?> newValue;
        if (type.isLink()) {
          newValue = new LinkSet(entity);
        } else {
          newValue = new TrackedSet<>(entity);
        }
        if (value instanceof Collection<?>) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) value);
        } else if (value instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) value).values());
        } else if (value instanceof String stringValue) {

          if (!stringValue.isEmpty()) {
            final var items = stringValue.split(",");
            for (var s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
        } else if (MultiValue.isMultiValue(value)) {
          // GENERIC MULTI VALUE
          for (var s : MultiValue.getMultiValueIterable(value)) {
            ((Collection<Object>) newValue).add(s);
          }
        } else {
          throw new IllegalArgumentException("Cannot convert value to set: " + value);
        }
        return (RET) newValue;
      } else {
        return (RET) value;
      }
    } else if (List.class.isAssignableFrom(fieldJavaClass)) {
      if (!(value instanceof List) ||
          type.isLink() && !(value instanceof LinkList)
          || !type.isLink() && (value instanceof LinkList)) {
        // CONVERT IT TO LIST
        final Collection<?> newValue;

        if (type.isLink()) {
          newValue = new LinkList(entity);
        } else {
          newValue = new TrackedList<>(entity);
        }

        if (value instanceof Collection) {
          ((Collection<Object>) newValue).addAll((Collection<Object>) value);
        } else if (value instanceof Map) {
          ((Collection<Object>) newValue).addAll(((Map<String, Object>) value).values());
        } else if (value instanceof String stringValue) {

          if (!stringValue.isEmpty()) {
            final var items = stringValue.split(",");
            for (var s : items) {
              ((Collection<Object>) newValue).add(s);
            }
          }
        } else if (MultiValue.isMultiValue(value)) {
          // GENERIC MULTI VALUE
          for (var s : MultiValue.getMultiValueIterable(value)) {
            ((Collection<Object>) newValue).add(s);
          }
        } else {
          throw new IllegalArgumentException("Cannot convert value to list: " + value);
        }
        return (RET) newValue;
      } else {
        return (RET) value;
      }
    } else if (Map.class.isAssignableFrom(fieldJavaClass)) {
      if (!(value instanceof Map<?, ?>) ||
          type.isLink() && !(value instanceof LinkMap)
          || !type.isLink() && (value instanceof LinkList)) {
        // CONVERT IT TO LIST
        final Map<String, ?> newValue;

        if (type.isLink()) {
          newValue = new LinkMap(entity);
        } else {
          newValue = new TrackedMap<>(entity);
        }

        if (value instanceof Map<?, ?>) {
          ((Map<String, Object>) newValue).putAll((Map<String, Object>) value);
        } else {
          throw new IllegalArgumentException("Cannot convert value to map: " + value);
        }

        return (RET) newValue;
      } else {
        return (RET) value;
      }

    } else if (value instanceof Enum) {
      // ENUM
      if (Number.class.isAssignableFrom(fieldJavaClass)) {
        value = ((Enum<?>) value).ordinal();
      } else {
        value = value.toString();
      }
      if (!(value instanceof String) && !fieldJavaClass.isAssignableFrom(value.getClass())) {
        throw new IllegalArgumentException(
            "Property '"
                + fieldName
                + "' of type '"
                + fieldJavaClass
                + "' cannot accept value of type: "
                + value.getClass());
      }
    } else if (Date.class.isAssignableFrom(fieldJavaClass)) {
      if (value instanceof String) {
        final var config = session.getStorageInfo().getConfiguration();

        DateFormat formatter;

        if (((String) value).length() > config.getDateFormat().length()) {
          // ASSUMES YOU'RE USING THE DATE-TIME FORMATTE
          formatter = config.getDateTimeFormatInstance();
        } else {
          formatter = config.getDateFormatInstance();
        }

        try {
          var newValue = formatter.parse((String) value);
          // _fieldValues.put(iFieldName, newValue);
          return (RET) newValue;
        } catch (ParseException pe) {
          final var dateFormat =
              ((String) value).length() > config.getDateFormat().length()
                  ? config.getDateTimeFormat()
                  : config.getDateFormat();
          throw BaseException.wrapException(
              new QueryParsingException(session.getDatabaseName(),
                  "Error on conversion of date '" + value + "' using the format: " + dateFormat),
              pe, session.getDatabaseName());
        }
      }
    }

    value = PropertyType.convert(session, value, fieldJavaClass);

    return (RET) value;
  }

  public static <RET> RET getFieldValue(DatabaseSessionInternal db, Object value,
      final String iFieldName) {
    var context = new BasicCommandContext();
    context.setDatabaseSession(db);

    return getFieldValue(db, value, iFieldName, context);
  }

  @SuppressWarnings("unchecked")
  public static <RET> RET getFieldValue(
      DatabaseSessionInternal db, Object value, final String iFieldName,
      @Nonnull final CommandContext iContext) {
    if (value == null) {
      return null;
    }

    final var fieldNameLength = iFieldName.length();
    if (fieldNameLength == 0) {
      return (RET) value;
    }

    var currentRecord = value instanceof Identifiable ? (Identifiable) value : null;

    var beginPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    var nextSeparatorPos = iFieldName.charAt(0) == '.' ? 1 : 0;
    var firstInChain = true;
    do {
      var nextSeparator = ' ';
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
            for (var o : MultiValue.getMultiValueIterable(value)) {
              if (o instanceof Identifiable) {
                var r = getFieldValue(db, o, iFieldName);
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
        final var end = findClosingBracketPosition(iFieldName, nextSeparatorPos);
        if (end == -1) {
          throw new IllegalArgumentException("Missed closed ']'");
        }

        var indexPart = iFieldName.substring(nextSeparatorPos + 1, end);
        if (indexPart.isEmpty()) {
          return null;
        }

        nextSeparatorPos = end;

        if (value instanceof CommandContext) {
          value = ((CommandContext) value).getVariables();
        }

        if (value instanceof Identifiable) {
          final var record =
              currentRecord instanceof Identifiable ? currentRecord.getRecord(db) : null;

          final var index = getIndexPart(iContext, indexPart);
          final var indexAsString = index != null ? index.toString() : null;

          final var indexParts =
              StringSerializerHelper.smartSplit(
                  indexAsString, ',', StringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final var indexRanges =
              StringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final var indexCondition =
              StringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
          // SINGLE VALUE
          {
            value = ((EntityImpl) record).field(indexAsString);
          } else if (indexParts.size() > 1) {
            // MULTI VALUE
            final var values = new Object[indexParts.size()];
            for (var i = 0; i < indexParts.size(); ++i) {
              values[i] = ((EntityImpl) record).field(
                  IOUtils.getStringContent(indexParts.get(i)));
            }
            value = values;
          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            var from = indexRanges.get(0);
            var to = indexRanges.get(1);

            final var entity = (EntityImpl) record;

            final var fieldNames = entity.fieldNames();
            final var rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final var rangeTo =
                to != null && !to.isEmpty()
                    ? Math.min(Integer.parseInt(to), fieldNames.length - 1)
                    : fieldNames.length - 1;

            final var values = new Object[rangeTo - rangeFrom + 1];

            for (var i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = entity.field(fieldNames[i]);
            }

            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final var conditionFieldName = indexCondition.get(0);
            var conditionFieldValue =
                RecordSerializerStringAbstract.getTypeValue(db, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = IOUtils.getStringContent(conditionFieldValue);
            }

            final var fieldValue = getFieldValue(db, currentRecord, conditionFieldName);

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
          final var index = getIndexPart(iContext, indexPart);
          final var indexAsString = index != null ? index.toString() : null;

          final var indexParts =
              StringSerializerHelper.smartSplit(
                  indexAsString, ',', StringSerializerHelper.DEFAULT_IGNORE_CHARS);
          final var indexRanges =
              StringSerializerHelper.smartSplit(indexAsString, '-', ' ');
          final var indexCondition =
              StringSerializerHelper.smartSplit(indexAsString, '=', ' ');

          final var map = (Map<String, ?>) value;
          if (indexParts.size() == 1 && indexCondition.size() == 1 && indexRanges.size() == 1)
          // SINGLE VALUE
          {
            value = map.get(index);
          } else if (indexParts.size() > 1) {
            // MULTI VALUE
            final var values = new Object[indexParts.size()];
            for (var i = 0; i < indexParts.size(); ++i) {
              values[i] = map.get(IOUtils.getStringContent(indexParts.get(i)));
            }
            value = values;
          } else if (indexRanges.size() > 1) {

            // MULTI VALUES RANGE
            var from = indexRanges.get(0);
            var to = indexRanges.get(1);

            final List<String> fieldNames = new ArrayList<String>(map.keySet());
            final var rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final var rangeTo =
                to != null && !to.isEmpty()
                    ? Math.min(Integer.parseInt(to), fieldNames.size() - 1)
                    : fieldNames.size() - 1;

            final var values = new Object[rangeTo - rangeFrom + 1];

            for (var i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = map.get(fieldNames.get(i));
            }

            value = values;

          } else if (!indexCondition.isEmpty()) {
            // CONDITION
            final var conditionFieldName = indexCondition.get(0);
            var conditionFieldValue =
                RecordSerializerStringAbstract.getTypeValue(db, indexCondition.get(1));

            if (conditionFieldValue instanceof String) {
              conditionFieldValue = IOUtils.getStringContent(conditionFieldValue);
            }

            final var fieldValue = map.get(conditionFieldName);

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
          final var index = getIndexPart(iContext, indexPart);
          final var indexAsString = index != null ? index.toString() : null;

          final var indexParts = StringSerializerHelper.smartSplit(indexAsString, ',');
          final var indexRanges = StringSerializerHelper.smartSplit(indexAsString, '-');
          final var indexCondition =
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
            final var values = new Object[indexParts.size()];
            for (var i = 0; i < indexParts.size(); ++i) {
              values[i] = MultiValue.getValue(value, Integer.parseInt(indexParts.get(i)));
            }
            if (indexParts.size() > 1) {
              value = values;
            } else {
              value = values[0];
            }

          } else if (isListOfNumbers(indexRanges)) {

            // MULTI VALUES RANGE
            var from = indexRanges.get(0);
            var to = indexRanges.get(1);

            final var rangeFrom = from != null && !from.isEmpty() ? Integer.parseInt(from) : 0;
            final int rangeTo;
            if (to != null && !to.isEmpty()) {
              rangeTo = Math.min(Integer.parseInt(to), MultiValue.getSize(value) - 1);
            } else {
              rangeTo = MultiValue.getSize(value) - 1;
            }

            var arraySize = rangeTo - rangeFrom + 1;
            if (arraySize < 0) {
              arraySize = 0;
            }
            final var values = new Object[arraySize];
            for (var i = rangeFrom; i <= rangeTo; ++i) {
              values[i - rangeFrom] = MultiValue.getValue(value, i);
            }
            value = values;

          } else {
            // CONDITION
            var pred = new SQLPredicate(iContext, indexAsString);
            final HashSet<Object> values = new LinkedHashSet<Object>();

            for (var v : MultiValue.getMultiValueIterable(value)) {
              if (v instanceof Identifiable) {
                var result =
                    pred.evaluate((Identifiable) v, ((Identifiable) v).getRecord(db), iContext);
                if (Boolean.TRUE.equals(result)) {
                  values.add(v);
                }
              } else if (v instanceof Map) {
                var entity = new EntityImpl(null);
                entity.updateFromMap((Map<String, ? extends Object>) v);
                var result = pred.evaluate(entity, entity, iContext);
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
          var executedMethod = false;
          if (!firstInChain && fieldName.endsWith("()")) {
            var method =
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
          final var indexCondition =
              StringSerializerHelper.smartSplit(fieldName, '=', ' ');

          if (indexCondition.size() == 2) {
            final var conditionFieldName = indexCondition.get(0);
            var conditionFieldValue =
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
            for (var v : MultiValue.getMultiValueIterable(value)) {
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
    var escaping = false;
    var innerBrackets = 0;
    var chars = iFieldName.toCharArray();
    for (var i = nextSeparatorPos + 1; i < chars.length; i++) {
      var next = chars[i];
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
    var firstChar = true;
    for (var c : indexAsString.toCharArray()) {
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
    for (var s : list) {
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
      final var ctxValue = iContext.getVariable(indexPart);
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
      final DBRecord rec;
      try {
        rec = ((Identifiable) iValue).getRecord(db);
      } catch (RecordNotFoundException rnf) {
        return null;
      }

      if (rec instanceof EntityImpl entity) {

        var fieldValue = entity.field(iConditionFieldName);

        if (iConditionFieldValue == null) {
          return fieldValue == null ? entity : null;
        }

        fieldValue = PropertyType.convert(db, fieldValue, iConditionFieldValue.getClass());
        if (fieldValue != null && fieldValue.equals(iConditionFieldValue)) {
          return entity;
        }
      }
    } else if (iValue instanceof Map<?, ?>) {
      final var map = (Map<String, ?>) iValue;
      var fieldValue = getMapEntry(db, map, iConditionFieldName);

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
      var pos = iName.indexOf('.');
      if (pos > -1) {
        iName = iName.substring(0, pos);
      }

      final var value = iMap.get(iName);
      if (value == null) {
        return null;
      }

      if (pos > -1) {
        final var restFieldName = iName.substring(pos + 1);
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

  public static Object getIdentifiableValue(@Nonnull DatabaseSessionInternal session,
      final Identifiable current,
      final String iFieldName) {
    if (iFieldName == null) {
      return null;
    }

    if (!iFieldName.isEmpty()) {
      final var begin = iFieldName.charAt(0);
      if (begin == '@') {
        if (session == null) {
          throw new IllegalStateException(
              "Custom attribute can not be processed because record is not bound to the session");
        }

        // RETURN AN ATTRIBUTE
        if (iFieldName.equalsIgnoreCase(ATTRIBUTE_THIS)) {
          return current.getRecord(session);
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID)) {
          return current.getIdentity();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_ID)) {
          return current.getIdentity().getClusterId();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RID_POS)) {
          return current.getIdentity().getClusterPosition();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_VERSION)) {
          return current.getRecord(session).getVersion();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_CLASS)) {
          return ((EntityImpl) current.getRecord(session)).getSchemaClassName();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_TYPE)) {
          return YouTrackDBEnginesManager.instance()
              .getRecordFactoryManager()
              .getRecordTypeName(
                  RecordInternal.getRecordType(session, current.getRecord(session)));
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_SIZE)) {
          final var stream = ((RecordAbstract) current.getRecord(session)).toStream();
          return stream != null ? stream.length : 0;
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_FIELDS)) {
          return ((EntityImpl) current.getRecord(session)).fieldNames();
        } else if (iFieldName.equalsIgnoreCase(ATTRIBUTE_RAW)) {
          return new String(((RecordAbstract) current.getRecord(session)).toStream());
        }
      }
    }
    if (current == null) {
      return null;
    }

    try {
      final EntityImpl entity = current.getRecord(session);
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

    final var function = iFunction.toUpperCase(Locale.ENGLISH);

    if (function.startsWith("SIZE(")) {
      result = currentValue instanceof DBRecord ? 1 : MultiValue.getSize(currentValue);
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
        final var bValue = ((Number) currentValue).intValue();
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
              DateHelper.getDateFormatInstance(iContext.getDatabaseSession())
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
              DateHelper.getDateTimeFormatInstance(iContext.getDatabaseSession())
                  .parse(currentValue.toString());
        } catch (ParseException ignore) {
        }
      }
    } else {
      // EXTRACT ARGUMENTS
      final var args =
          StringSerializerHelper.getParameters(iFunction.substring(iFunction.indexOf('(')));

      final var currentRecord =
          iContext != null ? (DBRecord) iContext.getVariable("$current") : null;
      for (var i = 0; i < args.size(); ++i) {
        final var arg = args.get(i);
        final var o = SQLHelper.getValue(arg, currentRecord, iContext);
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
          var formatter = new SimpleDateFormat(
              IOUtils.getStringContent(args.getFirst()));
          formatter.setTimeZone(DateHelper.getDatabaseTimeZone(iContext.getDatabaseSession()));
          result = formatter.format(currentValue);
        } else {
          result = String.format(IOUtils.getStringContent(args.getFirst()), currentValue);
        }
      } else if (function.startsWith("LEFT(")) {
        final var len = Integer.parseInt(args.getFirst());
        final var stringValue = currentValue.toString();
        result = stringValue.substring(0, Math.min(len, stringValue.length()));
      } else if (function.startsWith("RIGHT(")) {
        final var offset = Integer.parseInt(args.getFirst());
        final var stringValue = currentValue.toString();
        result =
            stringValue.substring(
                offset < stringValue.length() ? stringValue.length() - offset : 0);
      } else {
        final var f = SQLHelper.getFunction(iContext.getDatabaseSession(), null,
            iFunction);
        if (f != null) {
          result = f.execute(currentRecord, currentRecord, null, iContext);
        }
      }
    }

    return result;
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
            var mappedId = ridMapper.map(current.getIdentity());
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
    final var myMap = myFieldValue;
    final var otherMap = otherFieldValue;

    if (myMap.size() != otherMap.size()) {
      return false;
    }

    var oldMyAutoConvert = false;
    var oldOtherAutoConvert = false;

    final var myEntryIterator =
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
      final var myEntry =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Entry<Object, Object>>() {
                public Entry<Object, Object> call(DatabaseSessionInternal database) {
                  return myEntryIterator.next();
                }
              });
      final var myKey =
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
        final var myValue =
            makeDbCall(
                iMyDb,
                new DbRelatedCall<Object>() {
                  public Object call(DatabaseSessionInternal database) {
                    return myEntry.getValue();
                  }
                });

        final var otherValue =
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
    final var myCollection = myFieldValue;
    final var otherCollection = otherFieldValue;

    if (myCollection.size() != otherCollection.size()) {
      return false;
    }

    final var myIterator =
        makeDbCall(
            iMyDb,
            new DbRelatedCall<Iterator<?>>() {
              public Iterator<?> call(DatabaseSessionInternal database) {
                return myCollection.iterator();
              }
            });

    final var otherIterator =
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
      final var myNextVal =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Object>() {
                public Object call(DatabaseSessionInternal database) {
                  return myIterator.next();
                }
              });

      final var otherNextVal =
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
    final var mySet = myFieldValue;
    final var otherSet = otherFieldValue;

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

    final var myIterator =
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

      final var otherIterator =
          makeDbCall(
              iOtherDb,
              new DbRelatedCall<Iterator<?>>() {
                public Iterator<?> call(DatabaseSessionInternal database) {
                  return otherSet.iterator();
                }
              });

      final var myNextVal =
          makeDbCall(
              iMyDb,
              new DbRelatedCall<Object>() {
                public Object call(DatabaseSessionInternal database) {
                  return myIterator.next();
                }
              });

      var found = false;
      while (!found
          && makeDbCall(
          iOtherDb,
          new DbRelatedCall<Boolean>() {
            public Boolean call(DatabaseSessionInternal database) {
              return otherIterator.hasNext();
            }
          })) {
        final var otherNextVal =
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
    final var myBag = myFieldValue;
    final var otherBag = otherFieldValue;

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

    final var otherBagCopy =
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

    final var myIterator =
        makeDbCall(
            iMyDb,
            database -> myBag.iterator());

    while (makeDbCall(
        iMyDb,
        database -> myIterator.hasNext())) {
      final var myIdentifiable =
          makeDbCall(
              iMyDb,
              (DbRelatedCall<Identifiable>) database -> myIterator.next());

      final RID otherRid;
      if (ridMapper != null) {
        var convertedRid = ridMapper.map(myIdentifiable.getIdentity());
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

    if (myValue.getClass().isArray()) {
      final var myArraySize = Array.getLength(myValue);
      final var otherArraySize = Array.getLength(otherValue);

      if (myArraySize != otherArraySize) {
        return false;
      }

      for (var i = 0; i < myArraySize; i++) {
        final var first = Array.get(myValue, i);
        final var second = Array.get(otherValue, i);
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
        var convertedValue = ridMapper.map((RID) myValue);
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
