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

import com.orientechnologies.common.comparator.OCaseInsentiveComparator;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCollections;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.collate.ODefaultCollate;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexDefinition;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Contains the description of a persistent class property.
 */
public abstract class YTPropertyImpl implements YTProperty {

  protected final YTClassImpl owner;
  protected YTType linkedType;
  protected YTClass linkedClass;
  private transient String linkedClassName;

  protected String description;
  protected boolean mandatory;
  protected boolean notNull = false;
  protected String min;
  protected String max;
  protected String defaultValue;
  protected String regexp;
  protected boolean readonly;
  protected Map<String, String> customFields;
  protected OCollate collate = new ODefaultCollate();
  protected OGlobalProperty globalRef;

  private volatile int hashCode;

  public YTPropertyImpl(final YTClassImpl owner) {
    this.owner = owner;
  }

  public YTPropertyImpl(YTClassImpl oClassImpl, OGlobalProperty global) {
    this(oClassImpl);
    this.globalRef = global;
  }

  public String getName() {
    acquireSchemaReadLock();
    try {
      return globalRef.getName();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getFullName() {
    acquireSchemaReadLock();
    try {
      return owner.getName() + "." + globalRef.getName();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getFullNameQuoted() {
    acquireSchemaReadLock();
    try {
      return "`" + owner.getName() + "`.`" + globalRef.getName() + "`";
    } finally {
      releaseSchemaReadLock();
    }
  }

  public YTType getType() {
    acquireSchemaReadLock();
    try {
      return globalRef.getType();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public int compareTo(final YTProperty o) {
    acquireSchemaReadLock();
    try {
      return globalRef.getName().compareTo(o.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param session
   * @param iType   One of types supported.
   *                <ul>
   *                  <li>UNIQUE: Doesn't allow duplicates
   *                  <li>NOTUNIQUE: Allow duplicates
   *                  <li>FULLTEXT: Indexes single word for full text search
   *                </ul>
   * @return
   * @see {@link YTClass#createIndex(YTDatabaseSession, String,
   * YTClass.INDEX_TYPE, String...)} instead.
   */
  public OIndex createIndex(YTDatabaseSession session, final INDEX_TYPE iType) {
    return createIndex(session, iType.toString());
  }

  /**
   * Creates an index on this property. Indexes speed up queries but slow down insert and update
   * operations. For massive inserts we suggest to remove the index, make the massive insert and
   * recreate it.
   *
   * @param session
   * @param iType
   * @return
   * @see {@link YTClass#createIndex(YTDatabaseSession, String,
   * YTClass.INDEX_TYPE, String...)} instead.
   */
  public OIndex createIndex(YTDatabaseSession session, final String iType) {
    acquireSchemaReadLock();
    try {
      return owner.createIndex(session, getFullName(), iType, globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, INDEX_TYPE iType, YTDocument metadata) {
    return createIndex(session, iType.name(), metadata);
  }

  @Override
  public OIndex createIndex(YTDatabaseSession session, String iType, YTDocument metadata) {
    acquireSchemaReadLock();
    try {
      return owner.createIndex(session,
          getFullName(), iType, null, metadata, new String[]{globalRef.getName()});
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Remove the index on property
   *
   * @deprecated Use SQL command instead.
   */
  @Deprecated
  public YTPropertyImpl dropIndexes(YTDatabaseSessionInternal session) {
    session.checkSecurity(ORule.ResourceGeneric.SCHEMA, ORole.PERMISSION_DELETE);

    acquireSchemaReadLock();
    try {
      final OIndexManagerAbstract indexManager = session.getMetadata().getIndexManagerInternal();

      final ArrayList<OIndex> relatedIndexes = new ArrayList<OIndex>();
      for (final OIndex index : indexManager.getClassIndexes(session, owner.getName())) {
        final OIndexDefinition definition = index.getDefinition();

        if (OCollections.indexOf(
            definition.getFields(), globalRef.getName(), new OCaseInsentiveComparator())
            > -1) {
          if (definition instanceof OPropertyIndexDefinition) {
            relatedIndexes.add(index);
          } else {
            throw new IllegalArgumentException(
                "This operation applicable only for property indexes. "
                    + index.getName()
                    + " is "
                    + index.getDefinition());
          }
        }
      }

      for (final OIndex index : relatedIndexes) {
        session.getMetadata().getIndexManagerInternal().dropIndex(session, index.getName());
      }

      return this;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * Remove the index on property
   *
   * @deprecated by {@link YTProperty#dropIndexes(YTDatabaseSessionInternal)}
   */
  @Deprecated
  public void dropIndexesInternal(YTDatabaseSessionInternal session) {
    dropIndexes(session);
  }

  /**
   * Returns the first index defined for the property.
   *
   * @deprecated Use
   * {@link YTClass#getInvolvedIndexes(YTDatabaseSession,
   * String...)} instead.
   */
  @Deprecated
  public OIndex getIndex(YTDatabaseSession session) {
    acquireSchemaReadLock();
    try {
      Set<OIndex> indexes = owner.getInvolvedIndexes(session, globalRef.getName());
      if (indexes != null && !indexes.isEmpty()) {
        return indexes.iterator().next();
      }
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * @deprecated Use
   * {@link YTClass#getInvolvedIndexes(YTDatabaseSession,
   * String...)} instead.
   */
  @Deprecated
  public Set<OIndex> getIndexes(YTDatabaseSession session) {
    acquireSchemaReadLock();
    try {
      return owner.getInvolvedIndexes(session, globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  /**
   * @deprecated Use
   * {@link YTClass#areIndexed(YTDatabaseSession, String...)}
   * instead.
   */
  @Deprecated
  public boolean isIndexed(YTDatabaseSession session) {
    acquireSchemaReadLock();
    try {
      return owner.areIndexed(session, globalRef.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public YTClass getOwnerClass() {
    return owner;
  }

  /**
   * Returns the linked class in lazy mode because while unmarshalling the class could be not loaded
   * yet.
   *
   * @return
   */
  public YTClass getLinkedClass() {
    acquireSchemaReadLock();
    try {
      if (linkedClass == null && linkedClassName != null) {
        linkedClass = owner.owner.getClass(linkedClassName);
      }
      return linkedClass;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public static void checkSupportLinkedClass(YTType type) {
    if (type != YTType.LINK
        && type != YTType.LINKSET
        && type != YTType.LINKLIST
        && type != YTType.LINKMAP
        && type != YTType.EMBEDDED
        && type != YTType.EMBEDDEDSET
        && type != YTType.EMBEDDEDLIST
        && type != YTType.EMBEDDEDMAP
        && type != YTType.LINKBAG) {
      throw new OSchemaException("Linked class is not supported for type: " + type);
    }
  }

  public YTType getLinkedType() {
    acquireSchemaReadLock();
    try {
      return linkedType;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public static void checkLinkTypeSupport(YTType type) {
    if (type != YTType.EMBEDDEDSET && type != YTType.EMBEDDEDLIST && type != YTType.EMBEDDEDMAP) {
      throw new OSchemaException("Linked type is not supported for type: " + type);
    }
  }

  public boolean isNotNull() {
    acquireSchemaReadLock();
    try {
      return notNull;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isMandatory() {
    acquireSchemaReadLock();
    try {
      return mandatory;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public boolean isReadonly() {
    acquireSchemaReadLock();
    try {
      return readonly;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getMin() {
    acquireSchemaReadLock();
    try {
      return min;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getMax() {
    acquireSchemaReadLock();
    try {
      return max;
    } finally {
      releaseSchemaReadLock();
    }
  }

  protected static Object quoteString(String s) {
    if (s == null) {
      return "null";
    }
    String result = "\"" + (s.replaceAll("\\\\", "\\\\\\\\").replaceAll("\"", "\\\\\"")) + "\"";
    return result;
  }

  public String getDefaultValue() {
    acquireSchemaReadLock();
    try {
      return defaultValue;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getRegexp() {
    acquireSchemaReadLock();
    try {
      return regexp;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public String getCustom(final String iName) {
    acquireSchemaReadLock();
    try {
      if (customFields == null) {
        return null;
      }

      return customFields.get(iName);
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Map<String, String> getCustomInternal() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) {
        return Collections.unmodifiableMap(customFields);
      }
      return null;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void removeCustom(YTDatabaseSession session, final String iName) {
    setCustom(session, iName, null);
  }

  public Set<String> getCustomKeys() {
    acquireSchemaReadLock();
    try {
      if (customFields != null) {
        return customFields.keySet();
      }

      return new HashSet<String>();
    } finally {
      releaseSchemaReadLock();
    }
  }

  public Object get(final ATTRIBUTES attribute) {
    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    switch (attribute) {
      case LINKEDCLASS:
        return getLinkedClass();
      case LINKEDTYPE:
        return getLinkedType();
      case MIN:
        return getMin();
      case MANDATORY:
        return isMandatory();
      case READONLY:
        return isReadonly();
      case MAX:
        return getMax();
      case DEFAULT:
        return getDefaultValue();
      case NAME:
        return getName();
      case NOTNULL:
        return isNotNull();
      case REGEXP:
        return getRegexp();
      case TYPE:
        return getType();
      case COLLATE:
        return getCollate();
      case DESCRIPTION:
        return getDescription();
    }

    throw new IllegalArgumentException("Cannot find attribute '" + attribute + "'");
  }

  public void set(YTDatabaseSession session, final ATTRIBUTES attribute, final Object iValue) {
    if (attribute == null) {
      throw new IllegalArgumentException("attribute is null");
    }

    final String stringValue = iValue != null ? iValue.toString() : null;
    var sessionInternal = (YTDatabaseSessionInternal) session;

    switch (attribute) {
      case LINKEDCLASS:
        setLinkedClass(session, sessionInternal.getMetadata().getSchema().getClass(stringValue));
        break;
      case LINKEDTYPE:
        if (stringValue == null) {
          setLinkedType(session, null);
        } else {
          setLinkedType(session, YTType.valueOf(stringValue));
        }
        break;
      case MIN:
        setMin(session, stringValue);
        break;
      case MANDATORY:
        setMandatory(session, Boolean.parseBoolean(stringValue));
        break;
      case READONLY:
        setReadonly(session, Boolean.parseBoolean(stringValue));
        break;
      case MAX:
        setMax(session, stringValue);
        break;
      case DEFAULT:
        setDefaultValue(session, stringValue);
        break;
      case NAME:
        setName(session, stringValue);
        break;
      case NOTNULL:
        setNotNull(session, Boolean.parseBoolean(stringValue));
        break;
      case REGEXP:
        setRegexp(session, stringValue);
        break;
      case TYPE:
        setType(session, YTType.valueOf(stringValue.toUpperCase(Locale.ENGLISH)));
        break;
      case COLLATE:
        setCollate(session, stringValue);
        break;
      case CUSTOM:
        int indx = stringValue != null ? stringValue.indexOf('=') : -1;
        if (indx < 0) {
          if ("clear".equalsIgnoreCase(stringValue)) {
            clearCustom(session);
          } else {
            throw new IllegalArgumentException(
                "Syntax error: expected <name> = <value> or clear, instead found: " + iValue);
          }
        } else {
          String customName = stringValue.substring(0, indx).trim();
          String customValue = stringValue.substring(indx + 1).trim();
          if (isQuoted(customValue)) {
            customValue = removeQuotes(customValue);
          }
          if (customValue.isEmpty()) {
            removeCustom(session, customName);
          } else {
            setCustom(session, customName, customValue);
          }
        }
        break;
      case DESCRIPTION:
        setDescription(session, stringValue);
        break;
    }
  }

  private String removeQuotes(String s) {
    s = s.trim();
    return s.substring(1, s.length() - 1);
  }

  private boolean isQuoted(String s) {
    s = s.trim();
    if (s.startsWith("\"") && s.endsWith("\"")) {
      return true;
    }
    if (s.startsWith("'") && s.endsWith("'")) {
      return true;
    }
    return s.startsWith("`") && s.endsWith("`");
  }

  public OCollate getCollate() {
    acquireSchemaReadLock();
    try {
      return collate;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public YTProperty setCollate(YTDatabaseSession session, final OCollate collate) {
    setCollate(session, collate.getName());
    return this;
  }

  @Override
  public String getDescription() {
    acquireSchemaReadLock();
    try {
      return description;
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public String toString() {
    acquireSchemaReadLock();
    try {
      return globalRef.getName() + " (type=" + globalRef.getType() + ")";
    } finally {
      releaseSchemaReadLock();
    }
  }

  @Override
  public int hashCode() {
    int sh = hashCode;
    if (sh != 0) {
      return sh;
    }

    acquireSchemaReadLock();
    try {
      sh = hashCode;
      if (sh != 0) {
        return sh;
      }

      calculateHashCode();
      return hashCode;
    } finally {
      releaseSchemaReadLock();
    }
  }

  private void calculateHashCode() {
    final int prime = 31;
    int result = super.hashCode();
    result = prime * result + ((owner == null) ? 0 : owner.hashCode());
    hashCode = result;
  }

  @Override
  public boolean equals(final Object obj) {
    acquireSchemaReadLock();
    try {
      if (this == obj) {
        return true;
      }
      if (obj == null || !YTProperty.class.isAssignableFrom(obj.getClass())) {
        return false;
      }
      YTProperty other = (YTProperty) obj;
      if (owner == null) {
        if (other.getOwnerClass() != null) {
          return false;
        }
      } else if (!owner.equals(other.getOwnerClass())) {
        return false;
      }
      return this.getName().equals(other.getName());
    } finally {
      releaseSchemaReadLock();
    }
  }

  public void fromStream(YTDocument document) {
    String name = document.field("name");
    YTType type = null;
    if (document.field("type") != null) {
      type = YTType.getById(((Integer) document.field("type")).byteValue());
    }
    Integer globalId = document.field("globalId");
    if (globalId != null) {
      globalRef = owner.owner.getGlobalPropertyById(globalId);
    } else {
      if (type == null) {
        type = YTType.ANY;
      }
      globalRef = owner.owner.findOrCreateGlobalProperty(name, type);
    }

    mandatory = document.containsField("mandatory") ? (Boolean) document.field("mandatory") : false;
    readonly = document.containsField("readonly") ? (Boolean) document.field("readonly") : false;
    notNull = document.containsField("notNull") ? (Boolean) document.field("notNull") : false;
    defaultValue = document.containsField("defaultValue") ? document.field("defaultValue") : null;
    if (document.containsField("collate")) {
      collate = OSQLEngine.getCollate(document.field("collate"));
    }

    min = document.containsField("min") ? document.field("min") : null;
    max = document.containsField("max") ? document.field("max") : null;
    regexp = document.containsField("regexp") ? document.field("regexp") : null;
    linkedClassName = document.containsField("linkedClass") ? document.field("linkedClass") : null;
    linkedType =
        document.field("linkedType") != null
            ? YTType.getById(((Integer) document.field("linkedType")).byteValue())
            : null;
    if (document.containsField("customFields")) {
      customFields = document.field("customFields", YTType.EMBEDDEDMAP);
    } else {
      customFields = null;
    }
    description = document.containsField("description") ? document.field("description") : null;
  }

  public Collection<OIndex> getAllIndexes(YTDatabaseSession session) {
    acquireSchemaReadLock();
    try {
      final Set<OIndex> indexes = owner.getIndexes(session);
      final List<OIndex> indexList = new LinkedList<OIndex>();
      for (final OIndex index : indexes) {
        final OIndexDefinition indexDefinition = index.getDefinition();
        if (indexDefinition.getFields().contains(globalRef.getName())) {
          indexList.add(index);
        }
      }

      return indexList;
    } finally {
      releaseSchemaReadLock();
    }
  }

  public YTDocument toStream() {
    YTDocument document = new YTDocument();
    document.field("name", getName());
    document.field("type", getType().id);
    document.field("globalId", globalRef.getId());
    document.field("mandatory", mandatory);
    document.field("readonly", readonly);
    document.field("notNull", notNull);
    document.field("defaultValue", defaultValue);

    document.field("min", min);
    document.field("max", max);
    if (regexp != null) {
      document.field("regexp", regexp);
    } else {
      document.removeField("regexp");
    }
    if (linkedType != null) {
      document.field("linkedType", linkedType.id);
    }
    if (linkedClass != null || linkedClassName != null) {
      document.field("linkedClass", linkedClass != null ? linkedClass.getName() : linkedClassName);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        YTType.EMBEDDEDMAP);
    if (collate != null) {
      document.field("collate", collate.getName());
    }
    document.field("description", description);
    return document;
  }

  public void acquireSchemaReadLock() {
    owner.acquireSchemaReadLock();
  }

  public void releaseSchemaReadLock() {
    owner.releaseSchemaReadLock();
  }

  public void acquireSchemaWriteLock(YTDatabaseSessionInternal session) {
    owner.acquireSchemaWriteLock(session);
  }

  public void releaseSchemaWriteLock(YTDatabaseSessionInternal session) {
    calculateHashCode();
    owner.releaseSchemaWriteLock(session);
  }

  public static void checkEmbedded(YTDatabaseSessionInternal session) {
    if (session.isRemote()) {
      throw new OSchemaException(
          "'Internal' schema modification methods can be used only inside of embedded database");
    }
  }

  protected void checkForDateFormat(YTDatabaseSessionInternal session, final String iDateAsString) {
    if (iDateAsString != null) {
      if (globalRef.getType() == YTType.DATE) {
        try {
          ODateHelper.getDateFormatInstance(session).parse(iDateAsString);
        } catch (ParseException e) {
          throw OException.wrapException(
              new OSchemaException(
                  "Invalid date format while formatting date '" + iDateAsString + "'"),
              e);
        }
      } else if (globalRef.getType() == YTType.DATETIME) {
        try {
          ODateHelper.getDateTimeFormatInstance(session).parse(iDateAsString);
        } catch (ParseException e) {
          throw OException.wrapException(
              new OSchemaException(
                  "Invalid datetime format while formatting date '" + iDateAsString + "'"),
              e);
        }
      }
    }
  }

  @Override
  public Integer getId() {
    return globalRef.getId();
  }

  public YTDocument toNetworkStream() {
    YTDocument document = new YTDocument();
    document.setTrackingChanges(false);
    document.field("name", getName());
    document.field("type", getType().id);
    document.field("globalId", globalRef.getId());
    document.field("mandatory", mandatory);
    document.field("readonly", readonly);
    document.field("notNull", notNull);
    document.field("defaultValue", defaultValue);

    document.field("min", min);
    document.field("max", max);
    if (regexp != null) {
      document.field("regexp", regexp);
    } else {
      document.removeField("regexp");
    }
    if (linkedType != null) {
      document.field("linkedType", linkedType.id);
    }
    if (linkedClass != null || linkedClassName != null) {
      document.field("linkedClass", linkedClass != null ? linkedClass.getName() : linkedClassName);
    }

    document.field(
        "customFields",
        customFields != null && customFields.size() > 0 ? customFields : null,
        YTType.EMBEDDEDMAP);
    if (collate != null) {
      document.field("collate", collate.getName());
    }
    document.field("description", description);

    return document;
  }
}
