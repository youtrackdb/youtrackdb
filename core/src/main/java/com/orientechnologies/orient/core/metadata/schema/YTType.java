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
package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.OList;
import com.orientechnologies.orient.core.db.record.OMap;
import com.orientechnologies.orient.core.db.record.OSet;
import com.orientechnologies.orient.core.db.record.OTrackedList;
import com.orientechnologies.orient.core.db.record.OTrackedMap;
import com.orientechnologies.orient.core.db.record.OTrackedSet;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.YTEntityInternal;
import com.orientechnologies.orient.core.serialization.ODocumentSerializable;
import com.orientechnologies.orient.core.serialization.OSerializableStream;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.type.ODocumentWrapper;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Generic representation of a type.<br> allowAssignmentFrom accepts any class, but Array.class
 * means that the type accepts generic Arrays.
 */
public enum YTType {
  BOOLEAN("Boolean", 0, Boolean.class, new Class<?>[]{Number.class}),

  INTEGER("Integer", 1, Integer.class, new Class<?>[]{Number.class}),

  SHORT("Short", 2, Short.class, new Class<?>[]{Number.class}),

  LONG(
      "Long",
      3,
      Long.class,
      new Class<?>[]{
          Number.class,
      }),

  FLOAT("Float", 4, Float.class, new Class<?>[]{Number.class}),

  DOUBLE("Double", 5, Double.class, new Class<?>[]{Number.class}),

  DATETIME("Datetime", 6, Date.class, new Class<?>[]{Date.class, Number.class}),

  STRING("String", 7, String.class, new Class<?>[]{Enum.class}),

  BINARY("Binary", 8, byte[].class, new Class<?>[]{byte[].class}),

  EMBEDDED(
      "Embedded",
      9,
      Object.class,
      new Class<?>[]{ODocumentSerializable.class, OSerializableStream.class}),

  EMBEDDEDLIST(
      "EmbeddedList", 10, List.class, new Class<?>[]{List.class, OMultiCollectionIterator.class}),

  EMBEDDEDSET("EmbeddedSet", 11, Set.class, new Class<?>[]{Set.class}),

  EMBEDDEDMAP("EmbeddedMap", 12, Map.class, new Class<?>[]{Map.class}),

  /**
   * Links do not support link consistency and can be broken if you delete elements. If you wish to
   * keep link consistency in case you delete elements please consider to use edges instead.
   * {@link YTVertex#addEdge(YTVertex, YTClass)} or
   * {@link YTVertex#addLightWeightEdge(YTVertex, YTClass)} instead.
   */
  LINK("Link", 13, YTIdentifiable.class, new Class<?>[]{YTIdentifiable.class, YTRID.class}),

  /**
   * Link lists do not support link consistency and can keep broken links if you delete elements.
   * Also, they do not scale well if you have a high number of links.
   * <p>
   * If you wish to keep link consistency in case you delete elements please consider to use edges
   * instead. {@link YTVertex#addEdge(YTVertex, YTClass)} or
   * {@link YTVertex#addLightWeightEdge(YTVertex, YTClass)} instead.
   */
  LINKLIST("LinkList", 14, List.class, new Class<?>[]{List.class}),

  /**
   * Link sets do not support link consistency and can keep broken links if you delete elements.
   * Also, they do not scale well if you have a high number of links.
   * <p>
   * If you wish to keep link consistency in case you delete elements please consider to use edges
   * instead. {@link YTVertex#addEdge(YTVertex, YTClass)} or
   * {@link YTVertex#addLightWeightEdge(YTVertex, YTClass)} instead.
   */
  LINKSET("LinkSet", 15, Set.class, new Class<?>[]{Set.class}),

  /**
   * Link maps do not support link consistency and can keep broken links if you delete elements.
   * Also, they do not scale well if you have a high number of links.
   * <p>
   * If you wish to keep link consistency in case you delete elements please consider to use edges
   * instead. {@link YTVertex#addEdge(YTVertex, YTClass)} or
   * {@link YTVertex#addLightWeightEdge(YTVertex, YTClass)} instead.
   */
  LINKMAP("LinkMap", 16, Map.class, new Class<?>[]{Map.class}),

  BYTE("Byte", 17, Byte.class, new Class<?>[]{Number.class}),

  TRANSIENT("Transient", 18, null, new Class<?>[]{}),

  DATE("Date", 19, Date.class, new Class<?>[]{Number.class}),

  /**
   * @deprecated Deprecated and will be removed in next major release. Use {@link #BINARY} instead.
   */
  @Deprecated
  CUSTOM(
      "Custom",
      20,
      OSerializableStream.class,
      new Class<?>[]{OSerializableStream.class, Serializable.class}),

  DECIMAL("Decimal", 21, BigDecimal.class, new Class<?>[]{BigDecimal.class, Number.class}),

  LINKBAG("LinkBag", 22, ORidBag.class, new Class<?>[]{ORidBag.class}),

  ANY("Any", 23, null, new Class<?>[]{});

  // Don't change the order, the type discover get broken if you change the order.
  private static final YTType[] TYPES =
      new YTType[]{
          EMBEDDEDLIST, EMBEDDEDSET, EMBEDDEDMAP, LINK, CUSTOM, EMBEDDED, STRING, DATETIME
      };

  private static final YTType[] TYPES_BY_ID = new YTType[24];
  // Values previosly stored in javaTypes
  private static final Map<Class<?>, YTType> TYPES_BY_CLASS = new HashMap<>();

  static {
    for (YTType oType : values()) {
      TYPES_BY_ID[oType.id] = oType;
    }
    // This is made by hand because not all types should be add.
    TYPES_BY_CLASS.put(Boolean.class, BOOLEAN);
    TYPES_BY_CLASS.put(Boolean.TYPE, BOOLEAN);
    TYPES_BY_CLASS.put(Integer.TYPE, INTEGER);
    TYPES_BY_CLASS.put(Integer.class, INTEGER);
    TYPES_BY_CLASS.put(BigInteger.class, INTEGER);
    TYPES_BY_CLASS.put(Short.class, SHORT);
    TYPES_BY_CLASS.put(Short.TYPE, SHORT);
    TYPES_BY_CLASS.put(Long.class, LONG);
    TYPES_BY_CLASS.put(Long.TYPE, LONG);
    TYPES_BY_CLASS.put(Float.TYPE, FLOAT);
    TYPES_BY_CLASS.put(Float.class, FLOAT);
    TYPES_BY_CLASS.put(Double.TYPE, DOUBLE);
    TYPES_BY_CLASS.put(Double.class, DOUBLE);
    TYPES_BY_CLASS.put(Date.class, DATETIME);
    TYPES_BY_CLASS.put(String.class, STRING);
    TYPES_BY_CLASS.put(Enum.class, STRING);
    TYPES_BY_CLASS.put(byte[].class, BINARY);
    TYPES_BY_CLASS.put(Byte.class, BYTE);
    TYPES_BY_CLASS.put(Byte.TYPE, BYTE);
    TYPES_BY_CLASS.put(Character.class, STRING);
    TYPES_BY_CLASS.put(Character.TYPE, STRING);
    TYPES_BY_CLASS.put(YTRecordId.class, LINK);
    TYPES_BY_CLASS.put(BigDecimal.class, DECIMAL);
    TYPES_BY_CLASS.put(ORidBag.class, LINKBAG);
    TYPES_BY_CLASS.put(OTrackedSet.class, EMBEDDEDSET);
    TYPES_BY_CLASS.put(OSet.class, LINKSET);
    TYPES_BY_CLASS.put(OTrackedList.class, EMBEDDEDLIST);
    TYPES_BY_CLASS.put(OList.class, LINKLIST);
    TYPES_BY_CLASS.put(OTrackedMap.class, EMBEDDEDMAP);
    TYPES_BY_CLASS.put(OMap.class, LINKMAP);
    BYTE.castable.add(BOOLEAN);
    SHORT.castable.addAll(Arrays.asList(BOOLEAN, BYTE));
    INTEGER.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT));
    LONG.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER));
    FLOAT.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER));
    DOUBLE.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT));
    DECIMAL.castable.addAll(Arrays.asList(BOOLEAN, BYTE, SHORT, INTEGER, LONG, FLOAT, DOUBLE));
    LINKLIST.castable.add(LINKSET);
    EMBEDDEDLIST.castable.add(EMBEDDEDSET);
  }

  private final String name;
  final int id;
  private final Class<?> javaDefaultType;
  private final Class<?>[] allowAssignmentFrom;
  private final Set<YTType> castable;

  YTType(
      final String iName,
      final int iId,
      final Class<?> iJavaDefaultType,
      final Class<?>[] iAllowAssignmentBy) {
    name = iName;
    id = iId;
    javaDefaultType = iJavaDefaultType;
    allowAssignmentFrom = iAllowAssignmentBy;
    castable = new HashSet<>();
    castable.add(this);
  }

  /**
   * Return the type by ID.
   *
   * @param iId The id to search
   * @return The type if any, otherwise null
   */
  public static YTType getById(final byte iId) {
    if (iId >= 0 && iId < TYPES_BY_ID.length) {
      return TYPES_BY_ID[iId];
    }
    OLogManager.instance().warn(YTType.class, "Invalid type index: " + iId, (Object[]) null);
    return null;
  }

  /**
   * Get the identifier of the type. use this instead of {@link Enum#ordinal()} for guarantee a
   * cross code version identifier.
   *
   * @return the identifier of the type.
   */
  public final int getId() {
    return id;
  }

  /**
   * Return the correspondent type by checking the "assignability" of the class received as
   * parameter.
   *
   * @param iClass Class to check
   * @return YTType instance if found, otherwise null
   */
  public static YTType getTypeByClass(final Class<?> iClass) {
    if (iClass == null) {
      return null;
    }

    YTType type = TYPES_BY_CLASS.get(iClass);
    if (type != null) {
      return type;
    }
    type = getTypeByClassInherit(iClass);

    return type;
  }

  private static YTType getTypeByClassInherit(final Class<?> iClass) {
    if (iClass.isArray()) {
      return EMBEDDEDLIST;
    }
    int priority = 0;
    boolean comparedAtLeastOnce;
    do {
      comparedAtLeastOnce = false;
      for (final YTType type : TYPES) {
        if (type.allowAssignmentFrom.length > priority) {
          if (type.allowAssignmentFrom[priority].isAssignableFrom(iClass)) {
            return type;
          }
          comparedAtLeastOnce = true;
        }
      }

      priority++;
    } while (comparedAtLeastOnce);
    return null;
  }

  public static YTType getTypeByValue(Object value) {
    if (value == null) {
      return null;
    }
    Class<?> clazz = value.getClass();
    YTType type = TYPES_BY_CLASS.get(clazz);
    if (type != null) {
      return type;
    }

    YTType byType = getTypeByClassInherit(clazz);
    if (LINK == byType) {
      if (value instanceof YTDocument && ((YTDocument) value).isEmbedded()) {
        return EMBEDDED;
      }
    } else if (EMBEDDEDSET == byType) {
      if (checkLinkCollection(((Collection<?>) value))) {
        return LINKSET;
      }
    } else if (EMBEDDEDLIST == byType && !clazz.isArray()) {
      if (checkLinkCollection(((Collection<?>) value))) {
        return LINKLIST;
      }

    } else if (EMBEDDEDMAP == byType) {
      if (checkLinkCollection(((Map<?, ?>) value).values())) {
        return LINKMAP;
      }
    }
    return byType;
  }

  private static boolean checkLinkCollection(Collection<?> toCheck) {
    boolean empty = true;
    for (Object object : toCheck) {
      if (object != null
          && (!(object instanceof YTIdentifiable)
          || (object instanceof YTDocument && ((YTDocument) object).isEmbedded()))) {
        return false;
      } else if (object != null) {
        empty = false;
      }
    }
    return !empty;
  }

  public static boolean isSimpleType(final Object iObject) {
    if (iObject == null) {
      return false;
    }

    final Class<?> iType = iObject.getClass();

    return iType.isPrimitive()
        || Number.class.isAssignableFrom(iType)
        || String.class.isAssignableFrom(iType)
        || Boolean.class.isAssignableFrom(iType)
        || Date.class.isAssignableFrom(iType)
        || (iType.isArray()
        && (iType.equals(byte[].class)
        || iType.equals(char[].class)
        || iType.equals(int[].class)
        || iType.equals(long[].class)
        || iType.equals(double[].class)
        || iType.equals(float[].class)
        || iType.equals(short[].class)
        || iType.equals(Integer[].class)
        || iType.equals(String[].class)
        || iType.equals(Long[].class)
        || iType.equals(Short[].class)
        || iType.equals(Double[].class)));
  }

  /**
   * Convert types based on the iTargetClass parameter.
   *
   * @param value       Value to convert
   * @param targetClass Expected class
   * @return The converted value or the original if no conversion was applied
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public static <T> T convert(@Nullable YTDatabaseSession session, final Object value,
      final Class<? extends T> targetClass) {
    if (value == null) {
      return null;
    }

    if (targetClass == null) {
      return (T) value;
    }

    if (value.getClass().equals(targetClass))
    // SAME TYPE: DON'T CONVERT IT
    {
      return (T) value;
    }

    if (targetClass.isAssignableFrom(value.getClass()))
    // COMPATIBLE TYPES: DON'T CONVERT IT
    {
      return (T) value;
    }

    try {
      if (byte[].class.isAssignableFrom(targetClass)) {
        return (T) OStringSerializerHelper.getBinaryContent(value);
      } else if (byte[].class.isAssignableFrom(value.getClass())) {
        return (T) value;
      } else if (targetClass.isEnum()) {
        if (value instanceof Number) {
          return (T) ((Class<Enum>) targetClass).getEnumConstants()[((Number) value).intValue()];
        }
        return (T) Enum.valueOf((Class<Enum>) targetClass, value.toString());
      } else if (targetClass.equals(Byte.TYPE) || targetClass.equals(Byte.class)) {
        if (value instanceof Byte) {
          return (T) value;
        } else if (value instanceof String) {
          return (T) Byte.valueOf((String) value);
        } else {
          assert value instanceof Number;
          return (T) (Byte) ((Number) value).byteValue();
        }

      } else if (targetClass.equals(Short.TYPE) || targetClass.equals(Short.class)) {
        if (value instanceof Short) {
          return (T) value;
        } else if (value instanceof String) {
          return (T) Short.valueOf((String) value);
        } else if (value instanceof Number number) {
          return (T) (Short) number.shortValue();
        }

      } else if (targetClass.equals(Integer.TYPE) || targetClass.equals(Integer.class)) {
        if (value instanceof Integer) {
          return (T) value;
        } else if (value instanceof String) {
          if (value.toString().isEmpty()) {
            return null;
          }
          return (T) Integer.valueOf((String) value);
        } else if (value instanceof Number number) {
          return (T) (Integer) number.intValue();
        }

      } else if (targetClass.equals(Long.TYPE) || targetClass.equals(Long.class)) {
        if (value instanceof Long) {
          return (T) value;
        } else if (value instanceof String) {
          return (T) Long.valueOf((String) value);
        } else if (value instanceof Date) {
          return (T) (Long) ((Date) value).getTime();
        } else if (value instanceof Number number) {
          return (T) (Long) number.longValue();
        }

      } else if (targetClass.equals(Float.TYPE) || targetClass.equals(Float.class)) {
        if (value instanceof Float) {
          return (T) value;
        } else if (value instanceof String) {
          return (T) Float.valueOf((String) value);
        } else if (value instanceof Number number) {
          return (T) (Float) number.floatValue();
        }

      } else if (targetClass.equals(BigDecimal.class)) {
        if (value instanceof String) {
          return (T) new BigDecimal((String) value);
        } else if (value instanceof Number) {
          return (T) new BigDecimal(value.toString());
        }

      } else if (targetClass.equals(Double.TYPE) || targetClass.equals(Double.class)) {
        if (value instanceof Double) {
          return (T) value;
        } else if (value instanceof String) {
          return (T) Double.valueOf((String) value);
        } else if (value instanceof Float)
        // THIS IS NECESSARY DUE TO A BUG/STRANGE BEHAVIOR OF JAVA BY LOSSING PRECISION
        {
          return (T) Double.valueOf(value.toString());
        } else if (value instanceof Number number) {
          return (T) (Double) number.doubleValue();
        }

      } else if (targetClass.equals(Boolean.TYPE) || targetClass.equals(Boolean.class)) {
        if (value instanceof Boolean) {
          return (T) value;
        } else if (value instanceof String) {
          if (((String) value).equalsIgnoreCase("true")) {
            return (T) Boolean.TRUE;
          } else if (((String) value).equalsIgnoreCase("false")) {
            return (T) Boolean.FALSE;
          }
          throw new ODatabaseException(
              String.format("Error in conversion of value '%s' to type '%s'", value, targetClass));
        } else if (value instanceof Number) {
          return (T) (Boolean) (((Number) value).intValue() != 0);
        }

      } else if (Set.class.isAssignableFrom(targetClass)) {
        // The caller specifically wants a Set.  If the value is a collection
        // we will add all of the items in the collection to a set.  Otherwise
        // we will create a singleton set with only the value in it.
        if (value instanceof Collection<?>) {
          return (T) new HashSet<Object>((Collection<?>) value);
        } else {
          return (T) Collections.singleton(value);
        }

      } else if (List.class.isAssignableFrom(targetClass)) {
        // The caller specifically wants a List.  If the value is a collection
        // we will add all of the items in the collection to a List.  Otherwise
        // we will create a singleton List with only the value in it.
        if (value instanceof Collection<?>) {
          return (T) new ArrayList<>((Collection<Object>) value);
        } else {
          return (T) Collections.singletonList(value);
        }

      } else if (Collection.class.equals(targetClass)) {
        // The caller specifically wants a Collection of any type.
        // we will return a list if the value is a collection or
        // a singleton set if the value is not a collection.
        if (value instanceof Collection<?>) {
          return (T) new ArrayList<Object>((Collection<?>) value);
        } else {
          return (T) Collections.singleton(value);
        }

      } else if (targetClass.equals(Date.class)) {
        if (value instanceof Number) {
          return (T) new Date(((Number) value).longValue());
        }
        if (value instanceof String) {
          if (OIOUtils.isLong(value.toString())) {
            return (T) new Date(Long.parseLong(value.toString()));
          }
          try {
            return (T) ODateHelper.getDateTimeFormatInstance(
                    ODatabaseRecordThreadLocal.instance().get())
                .parse((String) value);
          } catch (ParseException ignore) {
            return (T) ODateHelper.getDateFormatInstance(
                    ODatabaseRecordThreadLocal.instance().get())
                .parse((String) value);
          }
        }
      } else if (targetClass.equals(String.class)) {
        if (value instanceof Collection
            && ((Collection) value).size() == 1
            && ((Collection) value).iterator().next() instanceof String) {
          return (T) ((Collection) value).iterator().next();
        }
        return (T) value.toString();
      } else if (YTIdentifiable.class.isAssignableFrom(targetClass)) {
        if (OMultiValue.isMultiValue(value)) {
          List<YTIdentifiable> result = new ArrayList<>();
          for (Object o : OMultiValue.getMultiValueIterable(value)) {
            if (o instanceof YTIdentifiable) {
              result.add((YTIdentifiable) o);
            } else if (o instanceof String) {
              try {
                result.add(new YTRecordId(value.toString()));
              } catch (Exception e) {
                throw OException.wrapException(
                    new ODatabaseException(
                        String.format(
                            "Error in conversion of value '%s' to type '%s'", value, targetClass)),
                    e);
              }
            } else if (o instanceof OResult res && res.isElement()) {
              result.add(res.getRecordId());
            }
          }

          if (result.isEmpty()) {
            return null;
          }
          if (result.size() == 1) {
            return (T) result.get(0);
          }

          throw new ODatabaseException(
              String.format(
                  "Error in conversion of value '%s' to type '%s'", value, targetClass));
        } else if (value instanceof String) {
          try {
            return (T) new YTRecordId((String) value);
          } catch (Exception e) {
            throw new ClassCastException(
                String.format(
                    "Error in conversion of value '%s' to type '%s'", value, targetClass));
          }
        }
      }

      if (targetClass.equals(ORidBag.class) && value instanceof Iterable<?> iterable) {
        var ridBag = new ORidBag((YTDatabaseSessionInternal) session);

        for (var item : iterable) {
          if (item instanceof YTIdentifiable identifiable) {
            ridBag.add(identifiable);
          } else if (item instanceof String) {
            ridBag.add(new YTRecordId((String) item));
          }
        }

        return (T) ridBag;
      }

      if (value instanceof Serializable && targetClass.equals(OSerializableStream.class)) {
        return (T) value;
      }

      if (targetClass.equals(YTIdentifiable.class) && value instanceof ODocumentWrapper wrapper) {
        if (session == null) {
          throw new ODatabaseException(
              "Cannot convert ODocumentWrapper to YTIdentifiable without a session");
        }
        return (T) wrapper.getDocument(session);
      }
    } catch (IllegalArgumentException e) {
      // PASS THROUGH
      throw OException.wrapException(
          new ODatabaseException(
              String.format("Error in conversion of value '%s' to type '%s'", value, targetClass)),
          e);
    } catch (Exception e) {
      if (value instanceof Collection
          && ((Collection) value).size() == 1
          && !Collection.class.isAssignableFrom(targetClass)) {
        // this must be a comparison with the result of a subquery, try to unbox the collection
        return convert(session, ((Collection) value).iterator().next(), targetClass);
      } else if (value instanceof OResult
          && ((OResult) value).getPropertyNames().size() == 1
          && !OResult.class.isAssignableFrom(targetClass)) {
        // try to unbox OResult with a single property, for subqueries
        return convert(session,
            ((OResult) value).getProperty(((OResult) value).getPropertyNames().iterator().next()),
            targetClass);
      } else if (value instanceof YTEntity
          && ((YTEntityInternal) value).getPropertyNamesInternal().size() == 1
          && !YTEntity.class.isAssignableFrom(targetClass)) {
        // try to unbox OResult with a single property, for subqueries
        return convert(session,
            ((YTEntity) value)
                .getProperty(
                    ((YTEntityInternal) value).getPropertyNamesInternal().iterator().next()),
            targetClass);
      }

      throw OException.wrapException(
          new ODatabaseException(
              String.format("Error in conversion of value '%s' to type '%s'", value, targetClass)),
          e);
    }

    throw new ODatabaseException(
        String.format("Error in conversion of value '%s' to type '%s'", value, targetClass));
  }

  public static Number increment(final Number a, final Number b) {
    if (a == null || b == null) {
      throw new IllegalArgumentException("Cannot increment a null value");
    }

    if (a instanceof Integer) {
      if (b instanceof Integer) {
        final int sum = a.intValue() + b.intValue();
        if (sum < 0 && a.intValue() > 0 && b.intValue() > 0)
        // SPECIAL CASE: UPGRADE TO LONG
        {
          return (long) (a.intValue() + b.intValue());
        }
        return sum;
      } else if (b instanceof Long) {
        return a.intValue() + b.longValue();
      } else if (b instanceof Short) {
        final int sum = a.intValue() + b.shortValue();
        if (sum < 0 && a.intValue() > 0 && b.shortValue() > 0)
        // SPECIAL CASE: UPGRADE TO LONG
        {
          return (long) (a.intValue() + b.shortValue());
        }
        return sum;
      } else if (b instanceof Float) {
        return a.intValue() + b.floatValue();
      } else if (b instanceof Double) {
        return a.intValue() + b.doubleValue();
      } else if (b instanceof BigDecimal) {
        return new BigDecimal(a.intValue()).add((BigDecimal) b);
      }

    } else if (a instanceof Long) {
      if (b instanceof Integer) {
        return a.longValue() + b.intValue();
      } else if (b instanceof Long) {
        return a.longValue() + b.longValue();
      } else if (b instanceof Short) {
        return a.longValue() + b.shortValue();
      } else if (b instanceof Float) {
        return a.longValue() + b.floatValue();
      } else if (b instanceof Double) {
        return a.longValue() + b.doubleValue();
      } else if (b instanceof BigDecimal) {
        return new BigDecimal(a.longValue()).add((BigDecimal) b);
      }

    } else if (a instanceof Short) {
      if (b instanceof Integer) {
        final int sum = a.shortValue() + b.intValue();
        if (sum < 0 && a.shortValue() > 0 && b.intValue() > 0)
        // SPECIAL CASE: UPGRADE TO LONG
        {
          return (long) (a.shortValue() + b.intValue());
        }
        return sum;
      } else if (b instanceof Long) {
        return a.shortValue() + b.longValue();
      } else if (b instanceof Short) {
        final int sum = a.shortValue() + b.shortValue();
        if (sum < 0 && a.shortValue() > 0 && b.shortValue() > 0)
        // SPECIAL CASE: UPGRADE TO INTEGER
        {
          return a.intValue() + b.intValue();
        }
        return sum;
      } else if (b instanceof Float) {
        return a.shortValue() + b.floatValue();
      } else if (b instanceof Double) {
        return a.shortValue() + b.doubleValue();
      } else if (b instanceof BigDecimal) {
        return new BigDecimal(a.shortValue()).add((BigDecimal) b);
      }

    } else if (a instanceof Float) {
      if (b instanceof Integer) {
        return a.floatValue() + b.intValue();
      } else if (b instanceof Long) {
        return a.floatValue() + b.longValue();
      } else if (b instanceof Short) {
        return a.floatValue() + b.shortValue();
      } else if (b instanceof Float) {
        return a.floatValue() + b.floatValue();
      } else if (b instanceof Double) {
        return a.floatValue() + b.doubleValue();
      } else if (b instanceof BigDecimal) {
        return BigDecimal.valueOf(a.floatValue()).add((BigDecimal) b);
      }

    } else if (a instanceof Double) {
      if (b instanceof Integer) {
        return a.doubleValue() + b.intValue();
      } else if (b instanceof Long) {
        return a.doubleValue() + b.longValue();
      } else if (b instanceof Short) {
        return a.doubleValue() + b.shortValue();
      } else if (b instanceof Float) {
        return a.doubleValue() + b.floatValue();
      } else if (b instanceof Double) {
        return a.doubleValue() + b.doubleValue();
      } else if (b instanceof BigDecimal) {
        return BigDecimal.valueOf(a.doubleValue()).add((BigDecimal) b);
      }

    } else if (a instanceof BigDecimal) {
      if (b instanceof Integer) {
        return ((BigDecimal) a).add(new BigDecimal(b.intValue()));
      } else if (b instanceof Long) {
        return ((BigDecimal) a).add(new BigDecimal(b.longValue()));
      } else if (b instanceof Short) {
        return ((BigDecimal) a).add(new BigDecimal(b.shortValue()));
      } else if (b instanceof Float) {
        return ((BigDecimal) a).add(BigDecimal.valueOf(b.floatValue()));
      } else if (b instanceof Double) {
        return ((BigDecimal) a).add(BigDecimal.valueOf(b.doubleValue()));
      } else if (b instanceof BigDecimal) {
        return ((BigDecimal) a).add((BigDecimal) b);
      }
    }

    throw new IllegalArgumentException(
        "Cannot increment value '"
            + a
            + "' ("
            + a.getClass()
            + ") with '"
            + b
            + "' ("
            + b.getClass()
            + ")");
  }

  public static Number[] castComparableNumber(Number context, Number max) {
    // CHECK FOR CONVERSION
    if (context instanceof Short) {
      // SHORT
      if (max instanceof Integer) {
        context = context.intValue();
      } else if (max instanceof Long) {
        context = context.longValue();
      } else if (max instanceof Float) {
        context = context.floatValue();
      } else if (max instanceof Double) {
        context = context.doubleValue();
      } else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.intValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, RoundingMode.DOWN);
        max = ((BigDecimal) max).setScale(maxScale, RoundingMode.DOWN);
      } else if (max instanceof Byte) {
        context = context.byteValue();
      }

    } else if (context instanceof Integer) {
      // INTEGER
      if (max instanceof Long) {
        context = context.longValue();
      } else if (max instanceof Float) {
        context = context.floatValue();
      } else if (max instanceof Double) {
        context = context.doubleValue();
      } else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.intValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, RoundingMode.DOWN);
        max = ((BigDecimal) max).setScale(maxScale, RoundingMode.DOWN);
      } else if (max instanceof Short) {
        max = max.intValue();
      } else if (max instanceof Byte) {
        max = max.intValue();
      }

    } else if (context instanceof Long) {
      // LONG
      if (max instanceof Float) {
        context = context.floatValue();
      } else if (max instanceof Double) {
        context = context.doubleValue();
      } else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.longValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, RoundingMode.DOWN);
        max = ((BigDecimal) max).setScale(maxScale, RoundingMode.DOWN);
      } else if (max instanceof Integer || max instanceof Byte || max instanceof Short) {
        max = max.longValue();
      }

    } else if (context instanceof Float) {
      // FLOAT
      if (max instanceof Double) {
        context = context.doubleValue();
      } else if (max instanceof BigDecimal) {
        context = BigDecimal.valueOf(context.floatValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, RoundingMode.DOWN);
        max = ((BigDecimal) max).setScale(maxScale, RoundingMode.DOWN);
      } else if (max instanceof Byte
          || max instanceof Short
          || max instanceof Integer
          || max instanceof Long) {
        max = max.floatValue();
      }

    } else if (context instanceof Double) {
      // DOUBLE
      if (max instanceof BigDecimal) {
        context = BigDecimal.valueOf(context.doubleValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, RoundingMode.DOWN);
        max = ((BigDecimal) max).setScale(maxScale, RoundingMode.DOWN);
      } else if (max instanceof Byte
          || max instanceof Short
          || max instanceof Integer
          || max instanceof Long
          || max instanceof Float) {
        max = max.doubleValue();
      }

    } else if (context instanceof BigDecimal) {
      // DOUBLE
      if (max instanceof Integer) {
        max = new BigDecimal((Integer) max);
      } else if (max instanceof Float) {
        max = BigDecimal.valueOf((Float) max);
      } else if (max instanceof Double) {
        max = BigDecimal.valueOf((Double) max);
      } else if (max instanceof Short) {
        max = new BigDecimal((Short) max);
      } else if (max instanceof Byte) {
        max = new BigDecimal((Byte) max);
      }

      int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
      context = ((BigDecimal) context).setScale(maxScale, RoundingMode.DOWN);
      max = ((BigDecimal) max).setScale(maxScale, RoundingMode.DOWN);
    } else if (context instanceof Byte) {
      if (max instanceof Short) {
        context = context.shortValue();
      } else if (max instanceof Integer) {
        context = context.intValue();
      } else if (max instanceof Long) {
        context = context.longValue();
      } else if (max instanceof Float) {
        context = context.floatValue();
      } else if (max instanceof Double) {
        context = context.doubleValue();
      } else if (max instanceof BigDecimal) {
        context = new BigDecimal(context.intValue());
        int maxScale = Math.max(((BigDecimal) context).scale(), (((BigDecimal) max).scale()));
        context = ((BigDecimal) context).setScale(maxScale, RoundingMode.DOWN);
        max = ((BigDecimal) max).setScale(maxScale, RoundingMode.DOWN);
      }
    }

    return new Number[]{context, max};
  }

  /**
   * Convert the input object to a string.
   *
   * @param iValue Any type supported
   * @return The string if the conversion succeed, otherwise the IllegalArgumentException exception
   */
  @Deprecated
  public static String asString(final Object iValue) {
    return iValue.toString();
  }

  public boolean isMultiValue() {
    return this == EMBEDDEDLIST
        || this == EMBEDDEDMAP
        || this == EMBEDDEDSET
        || this == LINKLIST
        || this == LINKMAP
        || this == LINKSET
        || this == LINKBAG;
  }

  public boolean isList() {
    return this == EMBEDDEDLIST || this == LINKLIST;
  }

  public boolean isLink() {
    return this == LINK
        || this == LINKSET
        || this == LINKLIST
        || this == LINKMAP
        || this == LINKBAG;
  }

  public boolean isEmbedded() {
    return this == EMBEDDED || this == EMBEDDEDLIST || this == EMBEDDEDMAP || this == EMBEDDEDSET;
  }

  public Class<?> getDefaultJavaType() {
    return javaDefaultType;
  }

  public Set<YTType> getCastable() {
    return castable;
  }

  public String getName() {
    return name;
  }
}
