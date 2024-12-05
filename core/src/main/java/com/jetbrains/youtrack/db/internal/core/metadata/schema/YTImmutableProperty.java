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

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.collate.OCollate;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationBinaryComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationCollectionComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationLinkbagComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationMapComparable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.validation.ValidationStringComparable;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @since 10/21/14
 */
public class YTImmutableProperty implements YTProperty {

  private final String name;
  private final String fullName;
  private final YTType type;
  private final String description;

  // do not make it volatile it is already thread safe.
  private YTClass linkedClass = null;

  private final String linkedClassName;

  private final YTType linkedType;
  private final boolean notNull;
  private final OCollate collate;
  private final boolean mandatory;
  private final String min;
  private final String max;
  private final String defaultValue;
  private final String regexp;
  private final Map<String, String> customProperties;
  private final YTClass owner;
  private final Integer id;
  private final boolean readOnly;
  private final Comparable<Object> minComparable;
  private final Comparable<Object> maxComparable;
  private final Set<OIndex> indexes;
  private final Collection<OIndex> allIndexes;

  public YTImmutableProperty(YTDatabaseSessionInternal session, YTProperty property,
      YTImmutableClass owner) {
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
      if (type.equals(YTType.STRING)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {
          minComparable = new ValidationStringComparable(conv);
        }
      } else if (type.equals(YTType.BINARY)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {
          minComparable = new ValidationBinaryComparable(conv);
        }
      } else if (type.equals(YTType.DATE)
          || type.equals(YTType.BYTE)
          || type.equals(YTType.SHORT)
          || type.equals(YTType.INTEGER)
          || type.equals(YTType.LONG)
          || type.equals(YTType.FLOAT)
          || type.equals(YTType.DOUBLE)
          || type.equals(YTType.DECIMAL)
          || type.equals(YTType.DATETIME)) {
        minComparable = (Comparable<Object>) safeConvert(session, min, type.getDefaultJavaType(),
            "min");
      } else if (type.equals(YTType.EMBEDDEDLIST)
          || type.equals(YTType.EMBEDDEDSET)
          || type.equals(YTType.LINKLIST)
          || type.equals(YTType.LINKSET)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {

          minComparable = new ValidationCollectionComparable(conv);
        }
      } else if (type.equals(YTType.LINKBAG)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {

          minComparable = new ValidationLinkbagComparable(conv);
        }
      } else if (type.equals(YTType.EMBEDDEDMAP) || type.equals(YTType.LINKMAP)) {
        Integer conv = safeConvert(session, min, Integer.class, "min");
        if (conv != null) {

          minComparable = new ValidationMapComparable(conv);
        }
      }
    }
    this.minComparable = minComparable;
    Comparable<Object> maxComparable = null;
    if (max != null) {
      if (type.equals(YTType.STRING)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationStringComparable(conv);
        }
      } else if (type.equals(YTType.BINARY)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationBinaryComparable(conv);
        }
      } else if (type.equals(YTType.DATE)) {
        // This is needed because a date is valid in any time range of the day.
        Date maxDate = (Date) safeConvert(session, max, type.getDefaultJavaType(), "max");
        if (maxDate != null) {
          Calendar cal = Calendar.getInstance();
          cal.setTime(maxDate);
          cal.add(Calendar.DAY_OF_MONTH, 1);
          maxDate = new Date(cal.getTime().getTime() - 1);
          maxComparable = (Comparable) maxDate;
        }
      } else if (type.equals(YTType.BYTE)
          || type.equals(YTType.SHORT)
          || type.equals(YTType.INTEGER)
          || type.equals(YTType.LONG)
          || type.equals(YTType.FLOAT)
          || type.equals(YTType.DOUBLE)
          || type.equals(YTType.DECIMAL)
          || type.equals(YTType.DATETIME)) {
        maxComparable = (Comparable<Object>) safeConvert(session, max, type.getDefaultJavaType(),
            "max");
      } else if (type.equals(YTType.EMBEDDEDLIST)
          || type.equals(YTType.EMBEDDEDSET)
          || type.equals(YTType.LINKLIST)
          || type.equals(YTType.LINKSET)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationCollectionComparable(conv);
        }
      } else if (type.equals(YTType.LINKBAG)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {

          maxComparable = new ValidationLinkbagComparable(conv);
        }
      } else if (type.equals(YTType.EMBEDDEDMAP) || type.equals(YTType.LINKMAP)) {
        Integer conv = safeConvert(session, max, Integer.class, "max");
        if (conv != null) {
          maxComparable = new ValidationMapComparable(conv);
        }
      }
    }
    this.maxComparable = maxComparable;
    this.indexes = property.getIndexes(session);
    this.allIndexes = property.getAllIndexes(session);
  }

  private <T> T safeConvert(YTDatabaseSessionInternal session, Object value, Class<T> target,
      String type) {
    T mc;
    try {
      mc = YTType.convert(session, value, target);
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
  public YTProperty setName(YTDatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public YTProperty setDescription(YTDatabaseSession session, String iDescription) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void set(YTDatabaseSession session, ATTRIBUTES attribute, Object iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTType getType() {
    return type;
  }

  @Override
  public YTClass getLinkedClass() {
    if (linkedClassName == null) {
      return null;
    }

    if (linkedClass != null) {
      return linkedClass;
    }

    YTSchema schema = ((YTImmutableClass) owner).getSchema();
    linkedClass = schema.getClass(linkedClassName);

    return linkedClass;
  }

  @Override
  public YTProperty setLinkedClass(YTDatabaseSession session, YTClass oClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTType getLinkedType() {
    return linkedType;
  }

  @Override
  public YTProperty setLinkedType(YTDatabaseSession session, YTType type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isNotNull() {
    return notNull;
  }

  @Override
  public YTProperty setNotNull(YTDatabaseSession session, boolean iNotNull) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OCollate getCollate() {
    return collate;
  }

  @Override
  public YTProperty setCollate(YTDatabaseSession session, String iCollateName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTProperty setCollate(YTDatabaseSession session, OCollate collate) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isMandatory() {
    return mandatory;
  }

  @Override
  public YTProperty setMandatory(YTDatabaseSession session, boolean mandatory) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isReadonly() {
    return readOnly;
  }

  @Override
  public YTProperty setReadonly(YTDatabaseSession session, boolean iReadonly) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMin() {
    return min;
  }

  @Override
  public YTProperty setMin(YTDatabaseSession session, String min) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getMax() {
    return max;
  }

  @Override
  public YTProperty setMax(YTDatabaseSession session, String max) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getDefaultValue() {
    return defaultValue;
  }

  @Override
  public YTProperty setDefaultValue(YTDatabaseSession session, String defaultValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, INDEX_TYPE iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, String iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, String iType, EntityImpl metadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, INDEX_TYPE iType, EntityImpl metadata) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTProperty dropIndexes(YTDatabaseSessionInternal session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<OIndex> getIndexes(YTDatabaseSession session) {
    return indexes;
  }

  @Override
  public OIndex getIndex(YTDatabaseSession session) {
    return indexes.iterator().next();
  }

  @Override
  public Collection<OIndex> getAllIndexes(YTDatabaseSession session) {
    return this.allIndexes;
  }

  @Override
  public boolean isIndexed(YTDatabaseSession session) {
    return owner.areIndexed(session, name);
  }

  @Override
  public String getRegexp() {
    return regexp;
  }

  @Override
  public YTProperty setRegexp(YTDatabaseSession session, String regexp) {
    throw new UnsupportedOperationException();
  }

  @Override
  public YTProperty setType(YTDatabaseSession session, YTType iType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getCustom(String iName) {
    return customProperties.get(iName);
  }

  @Override
  public YTProperty setCustom(YTDatabaseSession session, String iName, String iValue) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void removeCustom(YTDatabaseSession session, String iName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clearCustom(YTDatabaseSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Set<String> getCustomKeys() {
    return Collections.unmodifiableSet(customProperties.keySet());
  }

  @Override
  public YTClass getOwnerClass() {
    return owner;
  }

  @Override
  public Object get(ATTRIBUTES attribute) {
    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    switch (attribute) {
      case LINKEDCLASS:
        return getLinkedClass();
      case LINKEDTYPE:
        return linkedType;
      case MIN:
        return min;
      case MANDATORY:
        return mandatory;
      case READONLY:
        return readOnly;
      case MAX:
        return max;
      case DEFAULT:
        return defaultValue;
      case NAME:
        return name;
      case NOTNULL:
        return notNull;
      case REGEXP:
        return regexp;
      case TYPE:
        return type;
      case COLLATE:
        return collate;
      case DESCRIPTION:
        return description;
    }

    throw new IllegalArgumentException("Cannot find attribute '" + attribute + "'");
  }

  @Override
  public Integer getId() {
    return id;
  }

  @Override
  public int compareTo(YTProperty other) {
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
    if (!YTProperty.class.isAssignableFrom(obj.getClass())) {
      return false;
    }
    YTProperty other = (YTProperty) obj;
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
}
