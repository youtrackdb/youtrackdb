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
package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationBinaryComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationCollectionComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationLinkbagComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationMapComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationStringComparable;
import java.lang.reflect.Method;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * @since 10/21/14
 */
public class ImmutableSchemaProperty implements SchemaPropertyInternal {

  private final String name;
  private final String fullName;
  private final PropertyType type;
  private final String description;

  // do not make it volatile it is already thread safe.
  private SchemaClass linkedClass = null;

  private final String linkedClassName;

  private final PropertyType linkedType;
  private final boolean notNull;
  private final Collate collate;
  private final boolean mandatory;
  private final String min;
  private final String max;
  private final String defaultValue;
  private final String regexp;
  private final Map<String, String> customProperties;
  private final SchemaClass owner;
  private final Integer id;
  private final boolean readOnly;
  private final Comparable<Object> minComparable;
  private final Comparable<Object> maxComparable;
  private final Collection<Index> allIndexes;
  @Nonnull
  private final Method getter;
  @Nullable
  private final Method setter;

  public ImmutableSchemaProperty(DatabaseSessionInternal session, SchemaPropertyInternal property,
      SchemaImmutableClass owner) {
    name = property.getName();
    fullName = property.getFullName();
    type = property.getType();
    description = property.getDescription();

    if (property.getLinkedClass() != null) {
      linkedClassName = property.getLinkedClass().getName();
    } else {
      linkedClassName = null;
    }

    linkedType = property.getLinkedType();
    notNull = property.isNotNull();
    collate = property.getCollate();
    mandatory = property.isMandatory();
    min = property.getMin();
    max = property.getMax();
    defaultValue = property.getDefaultValue();
    regexp = property.getRegexp();
    customProperties = new HashMap<String, String>();

    for (String key : property.getCustomKeys()) {
      customProperties.put(key, property.getCustom(key));
    }

    this.owner = owner;
    id = property.getId();
    readOnly = property.isReadonly();
    Comparable<Object> minComparable = null;
    if (min != null) {
      if (type.equals(PropertyType.STRING)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {
          minComparable = new ValidationStringComparable(conv);
        }
      } else if (type.equals(PropertyType.BINARY)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {
          minComparable = new ValidationBinaryComparable(conv);
        }
      } else if (type.equals(PropertyType.DATE)
          || type.equals(PropertyType.BYTE)
          || type.equals(PropertyType.SHORT)
          || type.equals(PropertyType.INTEGER)
          || type.equals(PropertyType.LONG)
          || type.equals(PropertyType.FLOAT)
          || type.equals(PropertyType.DOUBLE)
          || type.equals(PropertyType.DECIMAL)
          || type.equals(PropertyType.DATETIME)) {
        minComparable = (Comparable<Object>) safeConvert(session, min, type.getDefaultJavaType(),
            "min");
      } else if (type.equals(PropertyType.EMBEDDEDLIST)
          || type.equals(PropertyType.EMBEDDEDSET)
          || type.equals(PropertyType.LINKLIST)
          || type.equals(PropertyType.LINKSET)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {

          minComparable = new ValidationCollectionComparable(conv);
        }
      } else if (type.equals(PropertyType.LINKBAG)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {

          minComparable = new ValidationLinkbagComparable(conv);
        }
      } else if (type.equals(PropertyType.EMBEDDEDMAP) || type.equals(PropertyType.LINKMAP)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {

          minComparable = new ValidationMapComparable(conv);
        }
      }
    }
    this.minComparable = minComparable;
    Comparable<Object> maxComparable = null;
    if (max != null) {
      if (type.equals(PropertyType.STRING)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationStringComparable(conv);
        }
      } else if (type.equals(PropertyType.BINARY)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationBinaryComparable(conv);
        }
      } else if (type.equals(PropertyType.DATE)) {
        // This is needed because a date is valid in any time range of the day.
        Date maxDate = (Date) safeConvert(session, max, type.getDefaultJavaType(), "max");
        if (maxDate != null) {
          Calendar cal = Calendar.getInstance();
          cal.setTime(maxDate);
          cal.add(Calendar.DAY_OF_MONTH, 1);
          maxDate = new Date(cal.getTime().getTime() - 1);
          maxComparable = (Comparable) maxDate;
        }
      } else if (type.equals(PropertyType.BYTE)
          || type.equals(PropertyType.SHORT)
          || type.equals(PropertyType.INTEGER)
          || type.equals(PropertyType.LONG)
          || type.equals(PropertyType.FLOAT)
          || type.equals(PropertyType.DOUBLE)
          || type.equals(PropertyType.DECIMAL)
          || type.equals(PropertyType.DATETIME)) {
        maxComparable = (Comparable<Object>) safeConvert(session, max, type.getDefaultJavaType(),
            "max");
      } else if (type.equals(PropertyType.EMBEDDEDLIST)
          || type.equals(PropertyType.EMBEDDEDSET)
          || type.equals(PropertyType.LINKLIST)
          || type.equals(PropertyType.LINKSET)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationCollectionComparable(conv);
        }
      } else if (type.equals(PropertyType.LINKBAG)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationLinkbagComparable(conv);
        }
      } else if (type.equals(PropertyType.EMBEDDEDMAP) || type.equals(PropertyType.LINKMAP)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {
          maxComparable = new ValidationMapComparable(conv);
        }
      }
    }

    this.maxComparable = maxComparable;
    this.allIndexes = property.getAllIndexesInternal(session);
    this.getter = property.getMaterializedGetter();
    this.setter = property.getMaterializedSetter();
  }

  private <T> T safeConvert(DatabaseSessionInternal session, Object value, Class<T> target,
      String type) {
    T mc;
    try {
      mc = PropertyType.convert(session, value, target);
    } catch (RuntimeException e) {
      LogManager.instance()
          .error(this, "Error initializing %s value check on property %s", e, type, fullName);
      mc = null;
    }
    return mc;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getFullName() {
    return fullName;
  }

  @Override
  public SchemaProperty setName(DatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public SchemaProperty setDescription(DatabaseSession session, String iDescription) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void set(DatabaseSession session, ATTRIBUTES attribute, Object iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PropertyType getType() {
    return type;
  }

  @Override
  public SchemaClass getLinkedClass() {
    if (linkedClassName == null) {
      return null;
    }

    if (linkedClass != null) {
      return linkedClass;
    }

    Schema schema = ((SchemaImmutableClass) owner).getSchema();
    linkedClass = schema.getClass(linkedClassName);

    return linkedClass;
  }

  @Override
  public SchemaProperty setLinkedClass(DatabaseSession session, SchemaClass oClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public PropertyType getLinkedType() {
    return linkedType;
  }

  @Override
  public SchemaProperty setLinkedType(DatabaseSession session, PropertyType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNotNull() {
    return notNull;
  }

  @Override
  public SchemaProperty setNotNull(DatabaseSession session, boolean iNotNull) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collate getCollate() {
    return collate;
  }

  @Override
  public SchemaProperty setCollate(DatabaseSession session, String iCollateName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaProperty setCollate(DatabaseSession session, Collate collate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMandatory() {
    return mandatory;
  }

  @Override
  public SchemaProperty setMandatory(DatabaseSession session, boolean mandatory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadonly() {
    return readOnly;
  }

  @Override
  public SchemaProperty setReadonly(DatabaseSession session, boolean iReadonly) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMin() {
    return min;
  }

  @Override
  public SchemaProperty setMin(DatabaseSession session, String min) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMax() {
    return max;
  }

  @Override
  public SchemaProperty setMax(DatabaseSession session, String max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDefaultValue() {
    return defaultValue;
  }

  @Override
  public SchemaProperty setDefaultValue(DatabaseSession session, String defaultValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String createIndex(DatabaseSession session, INDEX_TYPE iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String createIndex(DatabaseSession session, String iType) {
    throw new UnsupportedOperationException();
  }


  public String createIndex(DatabaseSession session, String iType, Map<String, ?> metadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String createIndex(DatabaseSession session, INDEX_TYPE iType, Map<String, ?> metadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<String> getAllIndexes(DatabaseSession session) {
    return this.allIndexes.stream().map(Index::getName).toList();
  }


  @Override
  public String getRegexp() {
    return regexp;
  }

  @Override
  public SchemaProperty setRegexp(DatabaseSession session, String regexp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SchemaProperty setType(DatabaseSession session, PropertyType iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCustom(String iName) {
    return customProperties.get(iName);
  }

  @Override
  public SchemaProperty setCustom(DatabaseSession session, String iName, String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeCustom(DatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearCustom(DatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(customProperties.keySet());
  }

  @Override
  public SchemaClass getOwnerClass() {
    return owner;
  }

  @Override
  public Object get(ATTRIBUTES attribute) {
    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    return switch (attribute) {
      case LINKEDCLASS -> getLinkedClass();
      case LINKEDTYPE -> linkedType;
      case MIN -> min;
      case MANDATORY -> mandatory;
      case READONLY -> readOnly;
      case MAX -> max;
      case DEFAULT -> defaultValue;
      case NAME -> name;
      case NOTNULL -> notNull;
      case REGEXP -> regexp;
      case TYPE -> type;
      case COLLATE -> collate;
      case DESCRIPTION -> description;
      default -> throw new IllegalArgumentException("Cannot find attribute '" + attribute + "'");
    };

  }

  @Override
  public Integer getId() {
    return id;
  }

  @Override
  public int compareTo(SchemaProperty other) {
    return name.compareTo(other.getName());
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((owner == null) ? 0 : owner.hashCode());
    return result;
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!super.equals(obj)) {
      return false;
    }
    if (!SchemaProperty.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    SchemaProperty other = (SchemaProperty) obj;
    if (owner == null) {
      return other.getOwnerClass() == null;
    } else {
      return owner.equals(other.getOwnerClass());
    }
  }

  @Override
  public String toString() {
    return name + " (type=" + type + ")";
  }

  public Comparable<Object> getMaxComparable() {
    return maxComparable;
  }

  public Comparable<Object> getMinComparable() {
    return minComparable;
  }

  @Override
  public Collection<Index> getAllIndexesInternal(@Nonnull DatabaseSession session) {
    return this.allIndexes;
  }

  @Override
  public void setMaterializedAccessMethods(@Nonnull Method getter, @Nullable Method setter) {

  }

  @Nonnull
  @Override
  public Method getMaterializedGetter() {
    return null;
  }

  @Nullable
  @Override
  public Method getMaterializedSetter() {
    return null;
  }
}
