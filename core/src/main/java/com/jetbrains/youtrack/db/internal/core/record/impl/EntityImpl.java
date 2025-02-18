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
import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Edge;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.StatefulEdge;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.GlobalProperty;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionAbstract;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent.ChangeType;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeTimeLine;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordElement;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMultiValue;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.QueryParsingException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.ClassIndexManager;
import com.jetbrains.youtrack.db.internal.core.iterator.EmptyMapEntryIterator;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ImmutableSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.ImmutableSchemaProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaShared;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Identity;
import com.jetbrains.youtrack.db.internal.core.metadata.security.PropertyAccess;
import com.jetbrains.youtrack.db.internal.core.metadata.security.PropertyEncryption;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordSchemaAware;
import com.jetbrains.youtrack.db.internal.core.record.RecordVersionHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.sql.SQLHelper;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLPredicate;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionOptimistic;
import java.lang.ref.WeakReference;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Entity representation to handle values dynamically. Can be used in schema-less, schema-mixed and
 * schema-full modes. Fields can be added at run-time. Instances can be reused across calls by using
 * the reset() before to re-use.
 */
@SuppressWarnings({"unchecked"})
public class EntityImpl extends RecordAbstract
    implements Iterable<Entry<String, Object>>, RecordSchemaAware, EntityInternal {

  public static final byte RECORD_TYPE = 'd';
  private static final String[] EMPTY_STRINGS = new String[]{};
  private int fieldSize;

  protected Map<String, EntityEntry> fields;

  private boolean trackingChanges = true;
  protected boolean ordered = true;
  private boolean lazyLoad = true;
  protected transient WeakReference<RecordElement> owner = null;

  protected ImmutableSchema schema;
  private String className;
  private SchemaImmutableClass immutableClazz;

  private int immutableSchemaVersion = 1;
  PropertyAccess propertyAccess;
  PropertyEncryption propertyEncryption;


  /**
   * Internal constructor used on unmarshalling.
   */
  public EntityImpl(DatabaseSessionInternal session) {
    super(session);
    assert session == null || session.assertIfNotActive();
    setup();
  }

  public EntityImpl(DatabaseSessionInternal database, RecordId rid) {
    super(database);
    assert assertIfAlreadyLoaded(rid);

    setup();

    this.recordId.setClusterId(rid.getClusterId());
    this.recordId.setClusterPosition(rid.getClusterPosition());
  }

  /**
   * Creates a new instance by the raw stream usually read from the database. New instances are not
   * persistent until {@link #save()} is called.
   *
   * @param iSource Raw stream
   */
  @Deprecated
  public EntityImpl(DatabaseSessionInternal session, final byte[] iSource) {
    super(session);
    source = iSource;
    setup();
  }

  /**
   * Creates a new instance in memory of the specified class, linked by the Record Id to the
   * persistent one. New instances are not persistent until {@link #save()} is called.
   *
   * @param iClassName Class name
   * @param recordId   Record Id
   */
  public EntityImpl(DatabaseSessionInternal session, final String iClassName,
      final RecordId recordId) {
    this(session, iClassName);

    assert assertIfAlreadyLoaded(recordId);

    this.recordId.setClusterId(recordId.getClusterId());
    this.recordId.setClusterPosition(recordId.getClusterPosition());

    final var database = getSession();
    if (this.recordId.getClusterId() > -1) {
      final Schema schema = database.getMetadata().getImmutableSchemaSnapshot();
      final var cls = schema.getClassByClusterId(this.recordId.getClusterId());
      if (cls != null && !cls.getName(session).equals(iClassName)) {
        throw new IllegalArgumentException(
            "Cluster id does not correspond class name should be "
                + iClassName
                + " but found "
                + cls.getName(session));
      }
    }

    dirty = 0;
    contentChanged = false;
    status = STATUS.NOT_LOADED;
  }


  /**
   * Creates a new instance in memory of the specified class. New instances are not persistent until
   * {@link #save()} is called.
   *
   * @param session    the session the instance will be attached to
   * @param iClassName Class name
   */
  public EntityImpl(DatabaseSessionInternal session, final String iClassName) {
    super(session);
    assert session == null || session.assertIfNotActive();
    setup();
    setClassName(iClassName);
  }

  /**
   * Creates a new instance in memory of the specified schema class. New instances are not
   * persistent until {@link #save()} is called. The database reference is taken from the thread
   * local.
   *
   * @param iClass SchemaClass instance
   */
  public EntityImpl(DatabaseSessionInternal session, final SchemaClass iClass) {
    this(session, iClass != null ? iClass.getName(session) : null);
  }

  /**
   * Fills a entity passing the field array in form of pairs of field name and value.
   *
   * @param iFields Array of field pairs
   */
  public EntityImpl(DatabaseSessionInternal session, final Object[] iFields) {
    this(session, DEFAULT_CLASS_NAME);

    if (iFields != null && iFields.length > 0) {
      for (var i = 0; i < iFields.length; i += 2) {
        field(iFields[i].toString(), iFields[i + 1]);
      }
    }
  }

  /**
   * Fills a entity passing a map of key/values where the key is the field name and the value the
   * field's value.
   *
   * @param iFieldMap Map of Object/Object
   */
  public EntityImpl(DatabaseSessionInternal session, final Map<?, Object> iFieldMap) {
    super(session);
    setup();

    if (iFieldMap != null && !iFieldMap.isEmpty()) {
      for (var entry : iFieldMap.entrySet()) {
        field(entry.getKey().toString(), entry.getValue());
      }
    }
  }

  /**
   * Fills a entity passing the field names/values pair, where the first pair is mandatory.
   */
  public EntityImpl(DatabaseSessionInternal session, final String iFieldName,
      final Object iFieldValue, final Object... iFields) {
    this(session, iFields);
    field(iFieldName, iFieldValue);
  }

  @Nullable
  @Override
  public Entity asEntity() {
    return this;
  }

  @Nullable
  @Override
  public Blob asBlob() {
    return null;
  }

  @Nullable
  @Override
  public DBRecord asRecord() {
    return this;
  }


  @Override
  @Nonnull
  public Vertex castToVertex() {
    if (this instanceof Vertex vertex) {
      return vertex;
    }

    SchemaClass type = this.getImmutableSchemaClass();
    if (type == null) {
      throw new IllegalStateException("Entity is not a vertex");
    }

    var session = getSession();
    if (type.isVertexType(session)) {
      return new VertexDelegate(this);
    }

    throw new IllegalStateException("Entity is not a vertex");
  }

  @Nullable
  @Override
  public Vertex asVertex() {
    if (this instanceof Vertex vertex) {
      return vertex;
    }

    SchemaClass type = this.getImmutableSchemaClass();
    if (type == null) {
      return null;
    }

    var session = getSession();
    if (type.isVertexType(session)) {
      return new VertexDelegate(this);
    }

    return null;
  }

  @Nonnull
  public StatefulEdge castToStatefulEdge() {
    if (this instanceof StatefulEdge edge) {
      return edge;
    }

    SchemaClass type = this.getImmutableSchemaClass();
    if (type == null) {
      throw new DatabaseException("Entity is not an edge");
    }
    var session = getSession();
    if (type.isEdgeType(session)) {
      return new StatefulEdgeImpl(this, session);
    }

    throw new DatabaseException("Entity is not an edge");
  }

  @Nullable
  public StatefulEdge asStatefulEdge() {
    if (this instanceof StatefulEdge edge) {
      return edge;
    }

    SchemaClass type = this.getImmutableSchemaClass();
    if (type == null) {
      return null;
    }
    var session = getSession();

    if (type.isEdgeType(session)) {
      return new StatefulEdgeImpl(this, session);
    }

    return null;
  }

  @Override
  public boolean isVertex() {
    if (this instanceof Vertex) {
      return true;
    }

    SchemaClass type = this.getImmutableSchemaClass();
    if (type == null) {
      return false;
    }

    var session = getSession();
    return type.isVertexType(session);
  }

  @Override
  public boolean isStatefulEdge() {
    if (this instanceof Edge) {
      return true;
    }

    SchemaClass type = this.getImmutableSchemaClass();
    if (type == null) {
      return false;
    }

    var session = getSession();
    return type.isEdgeType(session);
  }

  @Override
  public boolean isEdge() {
    return isStatefulEdge();
  }

  @Nonnull
  @Override
  public Edge castToEdge() {
    return castToStatefulEdge();
  }

  @Nullable
  @Override
  public Edge asEdge() {
    return asStatefulEdge();
  }

  @Override
  public boolean isBlob() {
    return false;
  }

  @Nonnull
  @Override
  public Blob castToBlob() {
    throw new DatabaseException("Entity is not a blob");
  }

  @Nonnull
  @Override
  public DBRecord castToRecord() {
    return this;
  }

  @Override
  public boolean isRecord() {
    return true;
  }

  @Override
  public boolean isProjection() {
    return false;
  }

  Set<String> calculatePropertyNames() {
    checkForBinding();

    var session = getSessionIfDefined();
    if (status == RecordElement.STATUS.LOADED
        && source != null
        && session != null
        && !session.isClosed()) {
      // DESERIALIZE FIELD NAMES ONLY (SUPPORTED ONLY BY BINARY SERIALIZER)
      final var fieldNames = recordFormat.getFieldNames(session, this, source);
      if (fieldNames != null) {
        Set<String> fields = new LinkedHashSet<>();
        if (propertyAccess != null && propertyAccess.hasFilters()) {
          for (var fieldName : fieldNames) {
            if (propertyAccess.isReadable(fieldName)) {
              fields.add(fieldName);
            }
          }
        } else {

          Collections.addAll(fields, fieldNames);
        }
        return fields;
      }
    }

    checkForFields();

    if (fields == null || fields.isEmpty()) {
      return Collections.emptySet();
    }

    Set<String> fields = new LinkedHashSet<>();
    if (propertyAccess != null && propertyAccess.hasFilters()) {
      for (var entry : this.fields.entrySet()) {
        if (entry.getValue().exists() && propertyAccess.isReadable(entry.getKey())) {
          fields.add(entry.getKey());
        }
      }
    } else {
      for (var entry : this.fields.entrySet()) {
        if (entry.getValue().exists()) {
          fields.add(entry.getKey());
        }
      }
    }

    return fields;
  }

  @Override
  public @Nonnull Collection<String> getPropertyNames() {
    return getPropertyNamesInternal();
  }

  @Override
  public boolean isEntity() {
    return true;
  }

  @Nonnull
  @Override
  public Entity castToEntity() {
    return this;
  }

  @Override
  public Collection<String> getPropertyNamesInternal() {
    return calculatePropertyNames();
  }

  /**
   * retrieves a property value from the current entity
   *
   * @param fieldName The field name, it can contain any character (it's not evaluated as an
   *                  expression, as in #eval()
   * @return the field value. Null if the field does not exist.
   */
  public <RET> RET getProperty(final @Nonnull String fieldName) {
    return getPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public Entity getEntityProperty(@Nonnull String name) {
    {
      var property = getProperty(name);

      var session = getSession();
      return switch (property) {
        case null -> null;
        case Entity entity -> entity;
        case Identifiable identifiable -> identifiable.getEntity(getSession());
        default -> throw new DatabaseException(session.getDatabaseName(),
            "Property "
                + name
                + " is not an entity property, it is a "
                + property.getClass().getName());
      };

    }
  }

  @Nullable
  @Override
  public Blob getBlobProperty(String propertyName) {
    var property = getProperty(propertyName);

    var session = getSession();
    return switch (property) {
      case null -> null;
      case Blob blob -> blob;
      case Identifiable identifiable -> identifiable.getBlob(session);
      default -> throw new DatabaseException(session.getDatabaseName(),
          "Property "
              + propertyName
              + " is not a blob property, it is a "
              + property.getClass().getName());
    };
  }

  @Override
  public <RET> RET getPropertyInternal(String name) {
    return getPropertyInternal(name, isLazyLoad());
  }

  @Override
  public <RET> RET getPropertyInternal(String name, boolean lazyLoad) {
    if (name == null) {
      return null;
    }

    checkForBinding();
    var session = getSessionIfDefined();

    var value = (RET) EntityHelper.getIdentifiableValue(session, this, name);
    if (!(!name.isEmpty() && name.charAt(0) == '@')
        && lazyLoad
        && value instanceof RID rid
        && (rid.isPersistent() || rid.isNew())) {
      // CREATE THE ENTITY OBJECT IN LAZY WAY
      try {
        value = session.load((RID) value);

        var entry = fields.get(name);
        entry.disableTracking(this, entry.value);
        entry.value = value;
        entry.enableTracking(this);
      } catch (RecordNotFoundException e) {
        return null;
      }
    }

    return convertToGraphElement(value);
  }

  @Override
  public <RET> RET getPropertyOnLoadValue(@Nonnull String name) {
    checkForBinding();

    Objects.requireNonNull(name, "Name argument is required.");
    VertexInternal.checkPropertyName(name);

    checkForFields();

    var field = fields.get(name);
    if (field != null) {
      var onLoadValue = (RET) field.getOnLoadValue(getSession());
      if (onLoadValue instanceof RidBag) {
        throw new IllegalArgumentException(
            "getPropertyOnLoadValue(name) is not designed to work with Edge properties");
      }
      if (onLoadValue instanceof RID orid) {
        if (isLazyLoad()) {
          try {
            return getSession().load(orid);
          } catch (RecordNotFoundException e) {
            return null;
          }
        } else {
          return onLoadValue;
        }
      }
      if (onLoadValue instanceof DBRecord record) {
        if (isLazyLoad()) {
          return onLoadValue;
        } else {
          return (RET) record.getIdentity();
        }
      }
      return onLoadValue;
    } else {
      return getPropertyInternal(name);
    }
  }

  private static <RET> RET convertToGraphElement(RET value) {
    if (value instanceof Entity) {
      if (((Entity) value).isVertex()) {
        value = (RET) ((Entity) value).castToVertex();
      } else {
        if (((Entity) value).isStatefulEdge()) {
          value = (RET) ((Entity) value).castToEdge();
        }
      }
    }
    return value;
  }

  @Nonnull
  @Override
  public StatefulEdge castToStateFullEdge() {
    if (this instanceof StatefulEdge edge) {
      return edge;
    }

    SchemaClass type = this.getImmutableSchemaClass();
    if (type == null) {
      throw new DatabaseException("Entity is not an edge");
    }
    var session = getSession();

    if (type.isEdgeType(session)) {
      return new StatefulEdgeImpl(this, session);
    }

    throw new DatabaseException("Entity is not an edge");
  }

  /**
   * This method similar to {@link Result#getProperty(String)} but unlike before mentioned method it
   * does not load link automatically.
   *
   * @param fieldName the name of the link property
   * @return the link property value, or null if the property does not exist
   * @throws IllegalArgumentException if requested property is not a link.
   * @see Result#getProperty(String)
   */
  @Nullable
  @Override
  public Identifiable getLinkProperty(@Nonnull String fieldName) {
    return getLinkPropertyInternal(fieldName);
  }

  @Nullable
  @Override
  public Identifiable getLinkPropertyInternal(String name) {
    if (name == null) {
      return null;
    }

    var result = accessProperty(name);
    if (result == null) {
      return null;
    }

    if (!(result instanceof Identifiable identifiable)
        || (result instanceof EntityImpl entity && entity.isEmbedded())) {
      throw new IllegalArgumentException("Requested property " + name + " is not a link.");
    }

    var id = identifiable.getIdentity();
    if (!(id.isPersistent() || id.isNew())) {
      throw new IllegalArgumentException("Requested property " + name + " is not a link.");
    }

    return (Identifiable) convertToGraphElement(result);
  }

  /**
   * retrieves a property value from the current entity, without evaluating it (eg. no conversion
   * from RID to entity)
   *
   * @param iFieldName The field name, it can contain any character (it's not evaluated as an
   *                   expression, as in #eval()
   * @return the field value. Null if the field does not exist.
   */
  <RET> RET getRawProperty(final String iFieldName) {
    checkForBinding();

    if (iFieldName == null) {
      return null;
    }

    return (RET) EntityHelper.getIdentifiableValue(getSessionIfDefined(), this, iFieldName);
  }

  /**
   * sets a property value on current entity
   *
   * @param iFieldName    The property name
   * @param propertyValue The property value
   */
  public void setProperty(final @Nonnull String iFieldName, @Nullable Object propertyValue) {
    if (propertyValue instanceof Collection<?> || propertyValue instanceof Map<?, ?>
        || propertyValue != null && propertyValue.getClass()
        .isArray()) {
      throw new DatabaseException(getSession().getDatabaseName(),
          "Property with name " + iFieldName +
              " is a multi-value property with class " + propertyValue.getClass().getName()
              + " and has to be created using appropriate getOrCreateXxx methods.");
    }

    setPropertyInternal(iFieldName, propertyValue);
  }

  @Override
  public @Nonnull <T> List<T> getOrCreateEmbeddedList(@Nonnull String name) {
    var value = this.<List<T>>getPropertyInternal(name);

    if (value == null) {
      value = new TrackedList<T>(this);
      setPropertyInternal(name, value, PropertyType.EMBEDDEDLIST);
    }

    return value;
  }

  @Nonnull
  @Override
  public <T> List<T> newEmbeddedList(@Nonnull String name) {
    var value = new TrackedList<T>(this);
    setPropertyInternal(name, value, PropertyType.EMBEDDEDLIST);
    return value;
  }

  @Override
  public @Nonnull <T> Set<T> getOrCreateEmbeddedSet(@Nonnull String name) {
    var value = this.<Set<T>>getPropertyInternal(name);
    if (value == null) {
      value = new TrackedSet<T>(this);
      setPropertyInternal(name, value, PropertyType.EMBEDDEDSET);
    }

    return value;
  }

  @Nonnull
  @Override
  public <T> Set<T> newEmbeddedSet(@Nonnull String name) {
    var value = new TrackedSet<T>(this);
    setPropertyInternal(name, value, PropertyType.EMBEDDEDSET);
    return value;
  }

  @Override
  public @Nonnull <T> Map<String, T> getOrCreateEmbeddedMap(@Nonnull String name) {
    var value = this.<Map<String, T>>getPropertyInternal(name);
    if (value == null) {
      value = new TrackedMap<T>(this);
      setPropertyInternal(name, value, PropertyType.EMBEDDEDMAP);
    }

    return value;
  }

  @Nonnull
  @Override
  public <T> Map<String, T> newEmbeddedMap(@Nonnull String name) {
    var value = new TrackedMap<T>(this);
    setPropertyInternal(name, value, PropertyType.EMBEDDEDMAP);
    return value;
  }

  @Override
  public @Nonnull List<Identifiable> getOrCreateLinkList(@Nonnull String name) {
    var value = this.<List<Identifiable>>getPropertyInternal(name);
    if (value == null) {
      value = new LinkList(this);
      setPropertyInternal(name, value, PropertyType.LINKLIST);
    }

    return value;
  }

  @Nonnull
  @Override
  public List<Identifiable> newLinkList(@Nonnull String name) {
    var value = new LinkList(this);
    setPropertyInternal(name, value, PropertyType.LINKLIST);
    return value;
  }

  @Nonnull
  @Override
  public Set<Identifiable> getOrCreateLinkSet(@Nonnull String name) {
    var value = this.<Set<Identifiable>>getPropertyInternal(name);
    if (value == null) {
      value = new LinkSet(this);
      setPropertyInternal(name, value, PropertyType.LINKSET);
    }

    return value;
  }

  @Nonnull
  @Override
  public Set<Identifiable> newLinkSet(@Nonnull String name) {
    var value = new LinkSet(this);
    setPropertyInternal(name, value, PropertyType.LINKSET);
    return value;
  }

  @Nonnull
  @Override
  public Map<String, Identifiable> getOrCreateLinkMap(@Nonnull String name) {
    var value = this.<Map<String, Identifiable>>getPropertyInternal(name);
    if (value == null) {
      value = new LinkMap(this);
      setPropertyInternal(name, value, PropertyType.LINKMAP);
    }

    return value;
  }

  @Nonnull
  @Override
  public Map<String, Identifiable> newLinkMap(@Nonnull String name) {
    var value = new LinkMap(this);
    setPropertyInternal(name, value, PropertyType.LINKMAP);
    return value;
  }

  @Override
  public void setPropertyInternal(String name, Object value) {
    if (value instanceof Entity entity
        && entity.getSchemaClass() == null
        && !((RecordId) entity.getIdentity()).isValid()) {
      setProperty(name, value, PropertyType.EMBEDDED);
    } else {
      setPropertyInternal(name, value, null);
    }
  }

  /**
   * Copies property values from one entity to another. Only properties with different values are
   * marked as dirty in result of such change. This rule is applied for all properties except of
   * <code>RidBag</code>. Only embedded <code>RidBag</code>s are compared but tree based are
   * always assigned to avoid performance overhead.
   *
   * @param from Entity from which properties are copied.
   */
  public void copyPropertiesFromOtherEntity(@Nonnull EntityImpl from) {
    checkForFields();
    from.deserializeFields();

    var fromFields = from.fields;
    if (fromFields == null || fromFields.isEmpty()) {
      return;
    }

    var sameCluster = from.recordId.getClusterId() == recordId.getClusterId();
    var session = getSession();

    for (var entry : fromFields.entrySet()) {
      if (entry.getValue().exists()) {
        var fromEntry = entry.getValue();

        var currentEntry = fields.get(entry.getKey());
        var currentValue = currentEntry != null ? currentEntry.value : null;
        var fromValue = fromEntry.value;

        if (fromValue != null && currentValue == null) {
          setPropertyInternal(entry.getKey(),
              copyRidBagIfNecessary(session, fromValue, sameCluster), fromEntry.type);
        } else if (fromValue == null && currentValue != null) {
          setPropertyInternal(entry.getKey(), null, currentEntry.type);
        } else if (fromValue.getClass() != currentValue.getClass()) {
          setPropertyInternal(entry.getKey(),
              copyRidBagIfNecessary(session, fromValue, sameCluster),
              fromEntry.type);
        } else {
          if (!(currentValue instanceof RidBag ridBag)) {
            if (!Objects.equals(fromEntry.type, currentEntry.type)) {
              setPropertyInternal(entry.getKey(), fromValue, fromEntry.type);
            }
          } else {
            if (ridBag.isEmbedded() || ((RidBag) fromValue).isEmbedded()) {
              if (!Objects.equals(fromEntry.type, currentEntry.type)) {
                setPropertyInternal(entry.getKey(),
                    copyRidBagIfNecessary(session,
                        copyRidBagIfNecessary(session, fromValue, sameCluster), sameCluster),
                    fromEntry.type);
              }
            } else {
              setPropertyInternal(entry.getKey(),
                  copyRidBagIfNecessary(session, fromValue, sameCluster), fromEntry.type);
            }
          }
        }
      }
    }
  }

  /**
   * All tree based ridbags are partitioned by clusters, so if we move entity to another cluster we
   * need to copy ridbags to avoid inconsistency.
   */
  private static Object copyRidBagIfNecessary(DatabaseSessionInternal seession, Object value,
      boolean sameCluster) {
    if (sameCluster) {
      return value;
    }

    if (!(value instanceof RidBag ridBag)) {
      return value;
    }

    if (ridBag.isEmbedded()) {
      return ridBag;
    }

    var ridBagCopy = new RidBag(seession);
    for (var rid : ridBag) {
      ridBagCopy.add(rid);
    }

    return ridBagCopy;
  }

  /**
   * Sets
   *
   * @param name          The property name
   * @param propertyValue The property value
   * @param types         Forced type (not auto-determined)
   */
  public void setProperty(@Nonnull String name, Object propertyValue, @Nonnull PropertyType types) {
    if (propertyValue instanceof Collection<?> || propertyValue instanceof Map<?, ?>
        || propertyValue != null && propertyValue.getClass()
        .isArray()) {
      throw new DatabaseException(getSession().getDatabaseName(),
          "Data containers have to be created using appropriate getOrCreateXxx methods");
    }

    setPropertyInternal(name, propertyValue, types);
  }

  public void compareAndSetPropertyInternal(String name, Object value, PropertyType type) {
    checkForBinding();

    var oldValue = getPropertyInternal(name);
    if (!Objects.equals(oldValue, value)) {
      setPropertyInternal(name, value, type);
    }
  }

  @Override
  public void setPropertyInternal(String name, Object value, PropertyType type) {
    checkForBinding();

    if (name == null) {
      throw new IllegalArgumentException("Field is null");
    }

    if (name.isEmpty()) {
      throw new IllegalArgumentException("Field name is empty");
    }

    if (value instanceof RecordAbstract recordAbstract) {
      recordAbstract.checkForBinding();

      if (recordAbstract.getSession() != getSession()) {
        throw new DatabaseException(getSession().getDatabaseName(),
            "Entity instance is bound to another session instance");
      }
    }

    final var begin = name.charAt(0);
    if (begin == '@') {
      switch (name.toLowerCase(Locale.ROOT)) {
        case EntityHelper.ATTRIBUTE_RID -> {
          if (status == STATUS.UNMARSHALLING) {
            recordId.fromString(value.toString());
          } else {
            throw new DatabaseException(getSession().getDatabaseName(),
                "Attribute " + EntityHelper.ATTRIBUTE_RID + " is read-only");
          }

        }
        case EntityHelper.ATTRIBUTE_VERSION -> {
          if (status == STATUS.UNMARSHALLING) {
            setVersion(Integer.parseInt(value.toString()));
          }
          throw new DatabaseException(getSession().getDatabaseName(),
              "Attribute " + EntityHelper.ATTRIBUTE_VERSION + " is read-only");
        }
        default -> {
          throw new DatabaseException(session.getDatabaseName(),
              "Attribute " + name + " can not be set");
        }
      }
    }

    checkForFields();

    var entry = fields.get(name);
    final boolean knownProperty;
    final Object oldValue;
    final PropertyType oldType;
    if (entry == null) {
      entry = new EntityEntry();
      fieldSize++;
      fields.put(name, entry);
      entry.markCreated();
      knownProperty = false;
      oldValue = null;
      oldType = null;
    } else {
      knownProperty = entry.exists();
      oldValue = entry.value;
      oldType = entry.type;
    }

    var fieldType = deriveFieldType(name, entry, type);
    if (value != null && fieldType != null) {
      value = EntityHelper.convertField(getSessionIfDefined(), this, name, fieldType, null,
          value);
    } else {
      if (value instanceof Enum) {
        value = value.toString();
      }
    }

    if (knownProperty)

    // CHECK IF IS REALLY CHANGED
    {
      if (value == null) {
        if (oldValue == null)
        // BOTH NULL: UNCHANGED
        {
          return;
        }
      } else {

        try {
          if (value.equals(oldValue)) {
            if (fieldType == oldType) {
              if (!(value instanceof RecordElement))
              // SAME BUT NOT TRACKABLE: SET THE RECORD AS DIRTY TO BE SURE IT'S SAVED
              {
                setDirty();
              }

              // SAVE VALUE: UNCHANGED
              return;
            }
          }
        } catch (Exception e) {
          LogManager.instance()
              .warn(
                  this,
                  "Error on checking the value of property %s against the record %s",
                  e,
                  name,
                  getIdentity());
        }
      }
    }

    if (oldValue instanceof
        RidBag ridBag) {
      ridBag.setOwner(null);
    } else {
      if (oldValue instanceof EntityImpl) {
        ((EntityImpl) oldValue).removeOwner(this);
      }
    }

    if (value != null) {
      if (value instanceof EntityImpl) {
        if (PropertyType.EMBEDDED.equals(fieldType)) {
          final var embeddedEntity = (EntityImpl) value;
          EntityInternalUtils.addOwner(embeddedEntity, this);
        }
      }

      if (value instanceof RidBag ridBag) {
        ridBag.setOwner(
            null); // in order to avoid IllegalStateException when ridBag changes the owner
        ridBag.setOwner(this);
        ridBag.setRecordAndField(recordId, name);
      }
    }


    if (oldType != fieldType) {
      // can be made in a better way, but "keeping type" issue should be solved before
      if (value == null || fieldType != null || oldType != PropertyType.getTypeByValue(value)) {
        entry.type = fieldType;
      }
    }
    entry.disableTracking(this, oldValue);
    if (value instanceof RID rid) {
      value = refreshNonPersistentRid(rid);
    }
    entry.value = value;
    if (!entry.exists()) {
      entry.setExists(true);
      fieldSize++;
    }
    entry.enableTracking(this);

    if (!entry.isChanged()) {
      entry.original = oldValue;
      entry.markChanged();
    }

    setDirty();
  }

  public <RET> RET removeProperty(@Nonnull final String iFieldName) {
    return removePropertyInternal(iFieldName);
  }

  @Override
  public <RET> RET removePropertyInternal(String name) {
    checkForBinding();
    checkForFields();

    if (EntityHelper.ATTRIBUTE_RID.equalsIgnoreCase(name)) {
      throw new DatabaseException(getSession().getDatabaseName(),
          "Attribute " + EntityHelper.ATTRIBUTE_RID + " is read-only");
    } else if (EntityHelper.ATTRIBUTE_VERSION.equalsIgnoreCase(name)) {
      if (EntityHelper.ATTRIBUTE_VERSION.equalsIgnoreCase(name)) {
        throw new DatabaseException(getSession().getDatabaseName(),
            "Attribute " + EntityHelper.ATTRIBUTE_VERSION + " is read-only");
      }
    } else if (EntityHelper.ATTRIBUTE_CLASS.equalsIgnoreCase(name)) {
      throw new DatabaseException(getSession().getDatabaseName(),
          "Attribute " + EntityHelper.ATTRIBUTE_CLASS + " is read-only");
    }

    final var entry = fields.get(name);
    if (entry == null) {
      return null;
    }

    var oldValue = entry.value;

    if (entry.exists() && trackingChanges) {
      // SAVE THE OLD VALUE IN A SEPARATE MAP
      if (entry.original == null) {
        entry.original = entry.value;
      }
      entry.value = null;
      entry.setExists(false);
      entry.markChanged();
    } else {
      fields.remove(name);
    }
    fieldSize--;
    entry.disableTracking(this, oldValue);
    if (oldValue instanceof RidBag) {
      ((RidBag) oldValue).setOwner(null);
    }

    setDirty();
    return (RET) oldValue;
  }

  private static void validateFieldsSecurity(DatabaseSessionInternal session,
      EntityImpl iRecord)
      throws ValidationException {
    if (session == null) {
      return;
    }

    iRecord.checkForBinding();
    iRecord = (EntityImpl) iRecord.getRecord(session);

    var security = session.getSharedContext().getSecurity();
    for (var mapEntry : iRecord.fields.entrySet()) {
      var entry = mapEntry.getValue();
      if (entry != null && (entry.isTxChanged() || entry.isTxTrackedModified())) {
        if (!security.isAllowedWrite(session, iRecord, mapEntry.getKey())) {
          throw new SecurityException(session.getDatabaseName(),
              String.format(
                  "Change of field '%s' is not allowed for user '%s'",
                  iRecord.getSchemaClassName() + "." + mapEntry.getKey(),
                  session.geCurrentUser().getName(session)));
        }
      }
    }
  }

  private static void validateField(
      DatabaseSessionInternal session, ImmutableSchema schema, EntityImpl iRecord,
      ImmutableSchemaProperty p)
      throws ValidationException {
    iRecord.checkForBinding();
    iRecord = (EntityImpl) iRecord.getRecord(session);

    final Object fieldValue;
    var entry = iRecord.fields.get(p.getName(session));
    if (entry != null && entry.exists()) {
      // AVOID CONVERSIONS: FASTER!
      fieldValue = entry.value;

      if (p.isNotNull(session) && fieldValue == null)
      // NULLITY
      {
        throw new ValidationException(session.getDatabaseName(),
            "The field '" + p.getFullName(session) + "' cannot be null, record: " + iRecord);
      }

      if (fieldValue != null && p.getRegexp(session) != null && p.getType(session)
          .equals(PropertyType.STRING)) {
        // REGEXP
        if (!((String) fieldValue).matches(p.getRegexp(session))) {
          throw new ValidationException(session.getDatabaseName(),
              "The field '"
                  + p.getFullName(session)
                  + "' does not match the regular expression '"
                  + p.getRegexp(session)
                  + "'. Field value is: "
                  + fieldValue
                  + ", record: "
                  + iRecord);
        }
      }

    } else {
      if (p.isMandatory(session)) {
        throw new ValidationException(session.getDatabaseName(),
            "The field '"
                + p.getFullName(session)
                + "' is mandatory, but not found on record: "
                + iRecord);
      }
      fieldValue = null;
    }

    final var type = p.getType(session);

    if (fieldValue != null && type != null) {
      // CHECK TYPE
      switch (type) {
        case LINK:
          validateLink(schema, session, p, fieldValue, false);
          break;
        case LINKLIST:
          if (!(fieldValue instanceof List)) {
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' has been declared as LINKLIST but an incompatible type is used. Value: "
                    + fieldValue);
          }
          validateLinkCollection(session, schema, p, (Collection<Object>) fieldValue, entry);
          break;
        case LINKSET:
          if (!(fieldValue instanceof Set)) {
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' has been declared as LINKSET but an incompatible type is used. Value: "
                    + fieldValue);
          }
          validateLinkCollection(session, schema, p, (Collection<Object>) fieldValue, entry);
          break;
        case LINKMAP:
          if (!(fieldValue instanceof Map)) {
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' has been declared as LINKMAP but an incompatible type is used. Value: "
                    + fieldValue);
          }
          validateLinkCollection(session, schema, p, ((Map<?, Object>) fieldValue).values(), entry);
          break;

        case LINKBAG:
          if (!(fieldValue instanceof RidBag)) {
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' has been declared as LINKBAG but an incompatible type is used. Value: "
                    + fieldValue);
          }
          validateLinkCollection(session, schema, p, (Iterable<Object>) fieldValue, entry);
          break;
        case EMBEDDED:
          validateEmbedded(session, p, fieldValue);
          break;
        case EMBEDDEDLIST:
          if (!(fieldValue instanceof List)) {
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' has been declared as EMBEDDEDLIST but an incompatible type is used. Value:"
                    + " "
                    + fieldValue);
          }
          if (p.getLinkedClass(session) != null) {
            for (var item : ((List<?>) fieldValue)) {
              validateEmbedded(session, p, item);
            }
          } else {
            if (p.getLinkedType(session) != null) {
              for (var item : ((List<?>) fieldValue)) {
                validateType(session, p, item);
              }
            }
          }
          break;
        case EMBEDDEDSET:
          if (!(fieldValue instanceof Set)) {
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' has been declared as EMBEDDEDSET but an incompatible type is used. Value: "
                    + fieldValue);
          }
          if (p.getLinkedClass(session) != null) {
            for (var item : ((Set<?>) fieldValue)) {
              validateEmbedded(session, p, item);
            }
          } else {
            if (p.getLinkedType(session) != null) {
              for (var item : ((Set<?>) fieldValue)) {
                validateType(session, p, item);
              }
            }
          }
          break;
        case EMBEDDEDMAP:
          if (!(fieldValue instanceof Map)) {
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' has been declared as EMBEDDEDMAP but an incompatible type is used. Value: "
                    + fieldValue);
          }
          if (p.getLinkedClass(session) != null) {
            for (var colleEntry : ((Map<?, ?>) fieldValue).entrySet()) {
              validateEmbedded(session, p, colleEntry.getValue());
            }
          } else {
            if (p.getLinkedType(session) != null) {
              for (var collEntry : ((Map<?, ?>) fieldValue).entrySet()) {
                validateType(session, p, collEntry.getValue());
              }
            }
          }
          break;
      }
    }

    if (p.getMin(session) != null && fieldValue != null) {
      // MIN
      final var min = p.getMin(session);
      if (p.getMinComparable().compareTo(fieldValue) > 0) {
        switch (p.getType(session)) {
          case STRING:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains fewer characters than "
                    + min
                    + " requested");
          case DATE:
          case DATETIME:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains the date "
                    + fieldValue
                    + " which precedes the first acceptable date ("
                    + min
                    + ")");
          case BINARY:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains fewer bytes than "
                    + min
                    + " requested");
          case EMBEDDEDLIST:
          case EMBEDDEDSET:
          case LINKLIST:
          case LINKSET:
          case EMBEDDEDMAP:
          case LINKMAP:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains fewer items than "
                    + min
                    + " requested");
          default:
            throw new ValidationException(session.getDatabaseName(),
                "The field '" + p.getFullName(session) + "' is less than " + min);
        }
      }
    }

    if (p.getMaxComparable() != null && fieldValue != null) {
      final var max = p.getMax(session);
      if (p.getMaxComparable().compareTo(fieldValue) < 0) {
        switch (p.getType(session)) {
          case STRING:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains more characters than "
                    + max
                    + " requested");
          case DATE:
          case DATETIME:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains the date "
                    + fieldValue
                    + " which is after the last acceptable date ("
                    + max
                    + ")");
          case BINARY:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains more bytes than "
                    + max
                    + " requested");
          case EMBEDDEDLIST:
          case EMBEDDEDSET:
          case LINKLIST:
          case LINKSET:
          case EMBEDDEDMAP:
          case LINKMAP:
            throw new ValidationException(session.getDatabaseName(),
                "The field '"
                    + p.getFullName(session)
                    + "' contains more items than "
                    + max
                    + " requested");
          default:
            throw new ValidationException(session.getDatabaseName(),
                "The field '" + p.getFullName(session) + "' is greater than " + max);
        }
      }
    }

    if (p.isReadonly(session) && !RecordVersionHelper.isTombstone(iRecord.getVersion())) {
      if (entry != null
          && (entry.isTxChanged() || entry.isTxTrackedModified())
          && !entry.isTxCreated()) {
        // check if the field is actually changed by equal.
        // this is due to a limitation in the merge algorithm used server side marking all
        // non-simple fields as dirty
        var orgVal = entry.getOnLoadValue(session);
        var simple =
            fieldValue != null ? PropertyType.isSimpleType(fieldValue)
                : PropertyType.isSimpleType(orgVal);
        if ((simple)
            || (fieldValue != null && orgVal == null)
            || (fieldValue == null && orgVal != null)
            || (fieldValue != null && !fieldValue.equals(orgVal))) {
          throw new ValidationException(session.getDatabaseName(),
              "The field '"
                  + p.getFullName(session)
                  + "' is immutable and cannot be altered. Field value is: "
                  + entry.value);
        }
      }
    }
  }

  private static void validateLinkCollection(
      DatabaseSessionInternal db, ImmutableSchema schema,
      final SchemaProperty property,
      Iterable<Object> values,
      EntityEntry value) {
    if (property.getLinkedClass(db) != null) {
      if (value.getTimeLine() != null) {
        var event =
            value.getTimeLine().getMultiValueChangeEvents();
        for (var object : event) {
          if (object.getChangeType() == ChangeType.ADD
              || object.getChangeType() == ChangeType.UPDATE
              && object.getValue() != null) {
            validateLink(schema, db, property, object.getValue(), true);
          }
        }
      } else {
        for (var object : values) {
          validateLink(schema, db, property, object, true);
        }
      }
    }
  }

  private static void validateType(DatabaseSessionInternal session, final SchemaProperty p,
      final Object value) {
    if (value != null) {
      if (PropertyType.convert(session, value, p.getLinkedType(session).getDefaultJavaType())
          == null) {
        throw new ValidationException(session.getDatabaseName(),
            "The field '"
                + p.getFullName(session)
                + "' has been declared as "
                + p.getType(session)
                + " of type '"
                + p.getLinkedType(session)
                + "' but the value is "
                + value);
      }
    }
  }

  private static void validateLink(
      ImmutableSchema schema, DatabaseSessionInternal db, final SchemaProperty p,
      final Object fieldValue, boolean allowNull) {
    if (fieldValue == null) {
      if (allowNull) {
        return;
      } else {
        throw new ValidationException(db.getDatabaseName(),
            "The field '"
                + p.getFullName(db)
                + "' has been declared as "
                + p.getType(db)
                + " but contains a null record (probably a deleted record?)");
      }
    }

    if (!(fieldValue instanceof Identifiable)) {
      throw new ValidationException(db.getDatabaseName(),
          "The field '"
              + p.getFullName(db)
              + "' has been declared as "
              + p.getType(db)
              + " but the value is not a record or a record-id");
    }

    final var schemaClass = p.getLinkedClass(db);
    if (schemaClass != null && !schemaClass.isSubClassOf(db, Identity.CLASS_NAME)) {
      // DON'T VALIDATE OUSER AND OROLE FOR SECURITY RESTRICTIONS
      var identifiable = (Identifiable) fieldValue;
      final var rid = identifiable.getIdentity();
      if (!schemaClass.hasPolymorphicClusterId(rid.getClusterId())) {
        // AT THIS POINT CHECK THE CLASS ONLY IF != NULL BECAUSE IN CASE OF GRAPHS THE RECORD
        // COULD BE PARTIAL
        SchemaClass cls;
        var clusterId = rid.getClusterId();
        if (clusterId != RID.CLUSTER_ID_INVALID) {
          cls = schema.getClassByClusterId(rid.getClusterId());
        } else if (identifiable instanceof Entity element) {
          cls = element.getSchemaClass();
        } else {
          cls = null;
        }

        if (cls != null && !schemaClass.isSuperClassOf(db, cls)) {
          throw new ValidationException(db.getDatabaseName(),
              "The field '"
                  + p.getFullName(db)
                  + "' has been declared as "
                  + p.getType(db)
                  + " of type '"
                  + schemaClass.getName(db)
                  + "' but the value is the entity "
                  + rid
                  + " of class '"
                  + cls
                  + "'");
        }
      }
    }
  }

  private static void validateEmbedded(DatabaseSessionInternal db, final SchemaProperty p,
      final Object fieldValue) {
    if (fieldValue == null) {
      return;
    }
    if (fieldValue instanceof RecordId) {
      throw new ValidationException(db.getDatabaseName(),
          "The field '"
              + p.getFullName(db)
              + "' has been declared as "
              + p.getType(db)
              + " but the value is the RecordID "
              + fieldValue);
    } else {
      if (fieldValue instanceof Identifiable embedded) {
        if (((RecordId) embedded.getIdentity()).isValid()) {
          throw new ValidationException(db.getDatabaseName(),
              "The field '"
                  + p.getFullName(db)
                  + "' has been declared as "
                  + p.getType(db)
                  + " but the value is a entity with the valid RecordID "
                  + fieldValue);
        }

        final var embeddedRecord = embedded.getRecord(db);
        if (embeddedRecord instanceof EntityImpl entity) {
          final var embeddedClass = p.getLinkedClass(db);
          if (entity.isVertex()) {
            throw new ValidationException(db.getDatabaseName(),
                "The field '"
                    + p.getFullName(db)
                    + "' has been declared as "
                    + p.getType(db)
                    + " with linked class '"
                    + embeddedClass
                    + "' but the record is of class '"
                    + entity.getImmutableSchemaClass().getName(db)
                    + "' that is vertex class");
          }

          if (entity.isStatefulEdge()) {
            throw new ValidationException(db.getDatabaseName(),
                "The field '"
                    + p.getFullName(db)
                    + "' has been declared as "
                    + p.getType(db)
                    + " with linked class '"
                    + embeddedClass
                    + "' but the record is of class '"
                    + entity.getImmutableSchemaClass().getName(db)
                    + "' that is edge class");
          }
        }

        final var embeddedClass = p.getLinkedClass(db);
        if (embeddedClass != null) {

          if (!(embeddedRecord instanceof EntityImpl entity)) {
            throw new ValidationException(db.getDatabaseName(),
                "The field '"
                    + p.getFullName(db)
                    + "' has been declared as "
                    + p.getType(db)
                    + " with linked class '"
                    + embeddedClass
                    + "' but the record was not a entity");
          }

          if (entity.getImmutableSchemaClass() == null) {
            throw new ValidationException(db.getDatabaseName(),
                "The field '"
                    + p.getFullName(db)
                    + "' has been declared as "
                    + p.getType(db)
                    + " with linked class '"
                    + embeddedClass
                    + "' but the record has no class");
          }

          if (!(entity.getImmutableSchemaClass().isSubClassOf(db, embeddedClass))) {
            throw new ValidationException(db.getDatabaseName(),
                "The field '"
                    + p.getFullName(db)
                    + "' has been declared as "
                    + p.getType(db)
                    + " with linked class '"
                    + embeddedClass
                    + "' but the record is of class '"
                    + entity.getImmutableSchemaClass().getName(db)
                    + "' that is not a subclass of that");
          }

          entity.validate();
        }

      } else {
        throw new ValidationException(db.getDatabaseName(),
            "The field '"
                + p.getFullName(db)
                + "' has been declared as "
                + p.getType(db)
                + " but an incompatible type is used. Value: "
                + fieldValue);
      }
    }
  }

  public boolean hasSameContentOf(final EntityImpl iOther) {
    iOther.checkForBinding();
    checkForBinding();
    final var currentDb = getSession();

    return EntityHelper.hasSameContentOf(this, currentDb, iOther, currentDb, null);
  }

  @Override
  public byte[] toStream() {
    checkForBinding();

    var prev = status;
    status = STATUS.MARSHALLING;
    try {
      if (source == null) {
        source = recordFormat.toStream(getSession(), this);
      }
    } finally {
      status = prev;
    }

    return source;
  }

  /**
   * Returns the entity as Map String,Object . If the entity has identity, then the @rid entry is
   * valued. If the entity has a class, then the @class entry is valued.
   *
   * @since 2.0
   */
  @Nonnull
  public Map<String, Object> toMap() {
    return toMap(true);
  }

  @Nonnull
  @Override
  public Map<String, Object> toMap(boolean includeMetadata) {
    checkForBinding();

    final Map<String, Object> map = new HashMap<>();

    for (var propertyName : getPropertyNamesInternal()) {
      var value = getPropertyInternal(propertyName);
      map.put(propertyName, ResultInternal.toMapValue(value, true));
    }

    if (includeMetadata) {
      if (isEmbedded()) {
        map.put(EntityHelper.ATTRIBUTE_EMBEDDED, true);
      } else {
        final var id = getIdentity();
        if (id.isValid()) {
          map.put(EntityHelper.ATTRIBUTE_RID, id);
        }
      }
      final var className = getSchemaClassName();
      if (className != null) {
        map.put(EntityHelper.ATTRIBUTE_CLASS, className);
      }
    }

    return map;
  }

  /**
   * Dumps the instance as string.
   */
  @Override
  public String toString() {
    if (isUnloaded()) {
      return "Unloaded record {" + getIdentity() + ", v" + getVersion() + "}";
    }

    return toString(new HashSet<>());
  }

  /**
   * Fills the EntityImpl directly with the string representation of the entity itself. Use it for
   * faster insertion but pay attention to respect the YouTrackDB record format.
   *
   * <p><code> record.reset();<br> record.setClassName("Account");<br>
   * record.fromString(new String("Account@id:" + data.getCyclesDone() +
   * ",name:'Luca',surname:'Garulli',birthDate:" + date.getTime()<br> + ",salary:" + 3000f +
   * i));<br> record.save();<br> </code>
   *
   * @param iValue String representation of the record.
   */
  @Deprecated
  public void fromString(final String iValue) {
    incrementLoading();
    try {
      dirty = 1;
      contentChanged = true;
      source = iValue.getBytes(StandardCharsets.UTF_8);

      removeAllCollectionChangeListeners();

      fields = null;
      fieldSize = 0;
    } finally {
      decrementLoading();
    }
  }

  /**
   * Returns the set of field names.
   */
  public String[] fieldNames() {
    return calculatePropertyNames().toArray(new String[]{});
  }

  /**
   * Returns the array of field values.
   */
  public Object[] fieldValues() {
    checkForBinding();

    checkForFields();
    final List<Object> res = new ArrayList<>(fields.size());
    for (var entry : fields.entrySet()) {
      if (entry.getValue().exists()
          && (propertyAccess == null || propertyAccess.isReadable(entry.getKey()))) {
        res.add(entry.getValue().value);
      }
    }
    return res.toArray();
  }

  public <RET> RET rawField(final String iFieldName) {
    if (iFieldName == null || iFieldName.isEmpty()) {
      return null;
    }

    checkForBinding();
    if (!checkForFields(iFieldName))
    // NO FIELDS
    {
      return null;
    }

    // NOT FOUND, PARSE THE FIELD NAME
    return EntityHelper.getFieldValue(getSession(), this, iFieldName);
  }

  /**
   * Evaluates a SQL expression against current entity. Example: <code> long amountPlusVat =
   * entity.eval("amount * 120 / 100");</code>
   *
   * @param iExpression SQL expression to evaluate.
   * @return The result of expression
   * @throws QueryParsingException in case the expression is not valid
   */
  public Object eval(final String iExpression) {
    checkForBinding();

    var context = new BasicCommandContext();
    context.setDatabaseSession(getSession());

    return eval(iExpression, context);
  }

  /**
   * Evaluates a SQL expression against current entity by passing a context. The expression can
   * refer to the variables contained in the context. Example: <code> CommandContext context = new
   * BasicCommandContext().setVariable("vat", 20); long amountPlusVat = entity.eval("amount *
   * (100+$vat) / 100", context); </code>
   *
   * @param iExpression SQL expression to evaluate.
   * @return The result of expression
   * @throws QueryParsingException in case the expression is not valid
   */
  public Object eval(final String iExpression, @Nonnull final CommandContext iContext) {
    checkForBinding();

    if (iContext.getDatabaseSession() != getSession()) {
      throw new DatabaseException(getSession().getDatabaseName(),
          "The context is bound to a different database instance, use the context from the same database instance");
    }

    return new SQLPredicate(iContext, iExpression).evaluate(this, null, iContext);
  }

  /**
   * Reads the field value.
   *
   * @param iFieldName field name
   * @return field value if defined, otherwise null
   */
  @Override
  public <RET> RET field(final String iFieldName) {
    checkForBinding();

    RET value = this.rawField(iFieldName);

    if (!(!iFieldName.isEmpty() && iFieldName.charAt(0) == '@')
        && lazyLoad
        && value instanceof RID
        && (((RID) value).isPersistent() || ((RID) value).isNew())) {
      // CREATE THE ENTITY OBJECT IN LAZY WAY
      var db = getSession();
      try {
        value = db.load((RID) value);

        if (!iFieldName.contains(".")) {
          var entry = fields.get(iFieldName);
          entry.disableTracking(this, entry.value);
          if (value instanceof RID rid) {
            entry.value = refreshNonPersistentRid(rid);
          } else {
            entry.value = value;
          }
          entry.enableTracking(this);
        }
      } catch (RecordNotFoundException e) {
        return null;
      }
    }
    return value;
  }

  /**
   * Reads the field value forcing the return type. Use this method to force return of RID instead
   * of the entire entity by passing RID.class as iFieldType.
   *
   * @param iFieldName field name
   * @param iFieldType Forced type.
   * @return field value if defined, otherwise null
   */
  public <RET> RET field(final String iFieldName, final Class<?> iFieldType) {
    checkForBinding();
    RET value = this.rawField(iFieldName);

    if (value != null) {
      value =
          EntityHelper.convertField(getSession()
              , this, iFieldName, PropertyType.getTypeByClass(iFieldType), iFieldType, value);
    }

    return value;
  }

  /**
   * Reads the field value forcing the return type. Use this method to force return of binary data.
   *
   * @param iFieldName field name
   * @param iFieldType Forced type.
   * @return field value if defined, otherwise null
   */
  public <RET> RET field(final String iFieldName, final PropertyType iFieldType) {
    checkForBinding();

    var session = getSessionIfDefined();
    RET value = field(iFieldName);
    PropertyType original;
    if (iFieldType != null && iFieldType != (original = getPropertyType(iFieldName))) {
      // this is needed for the csv serializer that don't give back values
      if (original == null) {
        original = PropertyType.getTypeByValue(value);
        if (iFieldType == original) {
          return value;
        }
      }

      final Object newValue;

      if (iFieldType == PropertyType.BINARY && value instanceof String) {
        newValue = StringSerializerHelper.getBinaryContent(value);
      } else {
        if (iFieldType == PropertyType.DATE && value instanceof Long) {
          newValue = new Date((Long) value);
        } else {
          if ((iFieldType == PropertyType.EMBEDDEDSET || iFieldType == PropertyType.LINKSET)
              && value instanceof List) {
            newValue =
                Collections.unmodifiableSet(
                    (Set<?>)
                        EntityHelper.convertField(getSession(), this, iFieldName, iFieldType,
                            null,
                            value));
          } else {
            if ((iFieldType == PropertyType.EMBEDDEDLIST || iFieldType == PropertyType.LINKLIST)
                && value instanceof Set) {
              newValue =
                  Collections.unmodifiableList(
                      (List<?>)
                          EntityHelper.convertField(session, this, iFieldName,
                              iFieldType, null, value));
            } else {
              if ((iFieldType == PropertyType.EMBEDDEDMAP || iFieldType == PropertyType.LINKMAP)
                  && value instanceof Map) {
                newValue =
                    Collections.unmodifiableMap(
                        (Map<?, ?>)
                            EntityHelper.convertField(session,
                                this, iFieldName, iFieldType, null, value));
              } else {
                newValue = PropertyType.convert(session, value, iFieldType.getDefaultJavaType());
              }
            }
          }
        }
      }

      if (newValue != null) {
        value = (RET) newValue;
      }
    }
    return value;
  }

  /**
   * Writes the field value. This method sets the current entity as dirty.
   *
   * @param iFieldName     field name. If contains dots (.) the change is applied to the nested
   *                       documents in chain.
   * @param iPropertyValue field value
   * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods
   * in chain.
   */
  public EntityImpl field(final String iFieldName, Object iPropertyValue) {
    return field(iFieldName, iPropertyValue, CommonConst.EMPTY_TYPES_ARRAY);
  }

  /**
   * Fills a entity passing the field names/values.
   */
  public EntityImpl fields(
      final String iFieldName, final Object iFieldValue, final Object... iFields) {
    checkForBinding();

    if (iFields != null && iFields.length % 2 != 0) {
      throw new IllegalArgumentException("Fields must be passed in pairs as name and value");
    }

    field(iFieldName, iFieldValue);
    if (iFields != null && iFields.length > 0) {
      for (var i = 0; i < iFields.length; i += 2) {
        field(iFields[i].toString(), iFields[i + 1]);
      }
    }
    return this;
  }

  /**
   * Deprecated. Use fromMap(Map) instead.<br> Fills a entity passing the field names/values as a
   * Map String,Object where the keys are the field names and the values are the field values.
   *
   * @see #updateFromMap(Map)
   */
  @Deprecated
  public EntityImpl fields(final Map<String, Object> iMap) {
    updateFromMap(iMap);
    return this;
  }

  /**
   * Fills a entity passing the field names/values as a Map String,Object where the keys are the
   * field names and the values are the field values. It accepts also @rid for record id and @class
   * for class name.
   *
   * @since 2.0
   */
  public void updateFromMap(@Nonnull final Map<String, ?> map) {
    checkForBinding();

    var db = getSession();
    var cls = getImmutableSchemaClass();

    status = STATUS.UNMARSHALLING;
    try {
      for (var entry : map.entrySet()) {
        var key = entry.getKey();
        if (key.isEmpty()) {
          continue;
        }
        if (key.charAt(0) == '@') {
          continue;
        }

        var property = cls != null ? cls.getProperty(db, key) : null;
        var type = property != null ? property.getType(db) : null;

        switch (type) {
          case LINKLIST: {
            updateLinkListFromMapEntry(entry, key);
            break;
          }
          case LINKSET: {
            updateLinkSetFromMapEntry(entry, key);
            break;
          }
          case LINKBAG: {
            updateLinkBagFromMapEntry(entry, db, key);
            break;
          }
          case LINKMAP: {
            updateLinkMapFromMapEntry(entry, key);
            break;
          }
          case EMBEDDEDLIST: {
            updateEmbeddedListFromMapEntry(db, entry, key);
            break;
          }
          case EMBEDDEDSET: {
            updateEmbeddedSetFromMapEntry(db, entry, key);
            break;
          }
          case EMBEDDEDMAP: {
            updateEmbeddedMapFromMapEntry(db, entry, key);
            break;
          }
          case EMBEDDED: {
            updateEmbeddedFromMapEntry(entry, db, key);
            break;
          }
          case null: {
            updatePropertyFromNonTypedMapEntry(entry, db, key);
            break;
          }
          default: {
            setPropertyInternal(key, entry.getValue());
          }
        }
      }
    } finally {
      status = STATUS.LOADED;
    }
  }

  private void updatePropertyFromNonTypedMapEntry(Entry<String, ?> entry,
      DatabaseSessionInternal session, String key) {
    var value = entry.getValue();
    value = convertMapValue(session, value);
    setPropertyInternal(key, value);
  }

  private Object convertMapValue(DatabaseSessionInternal session, Object value) {
    if (value instanceof Map<?, ?>) {
      var mapValue = (Map<String, ?>) value;

      var className = mapValue.get(EntityHelper.ATTRIBUTE_CLASS);
      var rid = mapValue.get(EntityHelper.ATTRIBUTE_RID);
      var embedded = mapValue.get(EntityHelper.ATTRIBUTE_EMBEDDED);

      if (embedded != null && Boolean.parseBoolean(embedded.toString())) {
        Entity embeddedEntity;
        if (className != null) {
          embeddedEntity = session.newEmbededEntity(className.toString());
        } else {
          embeddedEntity = session.newEmbededEntity();
        }

        embeddedEntity.updateFromMap(mapValue);
        value = embeddedEntity;
      } else if (rid != null) {
        var record = session.load(new RecordId(rid.toString()));
        if (record instanceof EntityImpl entity) {
          if (className != null && !className.equals(entity.getSchemaClassName())) {
            throw new IllegalArgumentException("Invalid  entity class name provided: "
                + className + " expected: " + entity.getSchemaClassName());
          }
          entity.updateFromMap(mapValue);
          value = entity;
        } else if (record instanceof Blob) {
          if (mapValue.size() > 1) {
            throw new IllegalArgumentException(
                "Invalid value for LINK: " + value);
          }
          value = record;
        } else {
          throw new IllegalArgumentException(
              "Invalid value, record expectd, provided : " + value);
        }
      } else if (className != null) {
        var entity = session.newEntity(className.toString());
        entity.updateFromMap(mapValue);
        value = entity;
      } else {
        var trackedMap = new TrackedMap<>(this);
        for (var mapEntry : mapValue.entrySet()) {
          trackedMap.put(mapEntry.getKey(), convertMapValue(session, mapEntry.getValue()));
        }
        value = trackedMap;
      }
    } else if (value instanceof List<?> list) {
      var trackedList = new TrackedList<>(this);
      for (var item : list) {
        trackedList.add(convertMapValue(session, item));
      }
      value = trackedList;
    } else if (value instanceof Set<?> set) {
      var trackedSet = new TrackedSet<>(this);
      for (var item : set) {
        trackedSet.add(convertMapValue(session, item));
      }
      value = trackedSet;
    }

    return value;
  }

  private void updateEmbeddedFromMapEntry(Entry<String, ?> entry, DatabaseSessionInternal session,
      String key) {
    Entity embedded;
    if (entry.getValue() instanceof Map<?, ?> mapValue) {
      embedded = new EntityImpl(session);
      embedded.updateFromMap((Map<String, ?>) mapValue);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for EMBEDDED: " + entry.getValue());
    }

    setPropertyInternal(key, embedded);
  }

  private void updateEmbeddedMapFromMapEntry(DatabaseSessionInternal session,
      Entry<String, ?> entry, String key) {
    if (entry.getValue() instanceof Map<?, ?> mapValue) {
      var embeddedMap = new TrackedMap<>(this);
      for (var mapEntry : mapValue.entrySet()) {
        embeddedMap.put(mapEntry.getKey().toString(),
            convertMapValue(session, mapEntry.getValue()));
      }
      setPropertyInternal(key, embeddedMap);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for EMBEDDEDMAP: " + entry.getValue());
    }
  }

  private void updateEmbeddedSetFromMapEntry(DatabaseSessionInternal session,
      Entry<String, ?> entry, String key) {
    if (entry.getValue() instanceof Collection<?> collection) {
      var embeddedSet = new TrackedSet<>(this);
      for (var item : collection) {
        embeddedSet.add(convertMapValue(session, item));
      }
      setPropertyInternal(key, embeddedSet);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for EMBEDDEDSET: " + entry.getValue());
    }
  }

  private void updateEmbeddedListFromMapEntry(DatabaseSessionInternal session,
      Entry<String, ?> entry, String key) {
    if (entry.getValue() instanceof Collection<?> collection) {
      var embeddedList = new TrackedList<>(this);
      for (var item : collection) {
        embeddedList.add(convertMapValue(session, item));
      }
      setPropertyInternal(key, embeddedList);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for EMBEDDEDLIST: " + entry.getValue());
    }
  }

  private void updateLinkMapFromMapEntry(Entry<String, ?> entry, String key) {
    if (entry.getValue() instanceof Map<?, ?> mapValue) {
      var linkMap = new LinkMap(this);
      for (var mapEntry : mapValue.entrySet()) {
        if (mapEntry.getKey() instanceof String keyString) {
          if (mapEntry.getValue() instanceof Identifiable identifiable) {
            linkMap.put(keyString, identifiable);
          } else {
            throw new IllegalArgumentException(
                "Invalid value for LINKMAP: " + mapEntry.getValue());
          }
        } else {
          throw new IllegalArgumentException(
              "Invalid key for LINKMAP: " + mapEntry.getKey());
        }
      }
      setPropertyInternal(key, linkMap);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for LINKMAP: " + entry.getValue());
    }
  }

  private void updateLinkBagFromMapEntry(Entry<String, ?> entry, DatabaseSessionInternal session,
      String key) {
    if (entry.getValue() instanceof Collection<?> collection) {
      var linkBag = new RidBag(session);
      for (var item : collection) {
        if (item instanceof Identifiable identifiable) {
          linkBag.add(identifiable.getIdentity());
        } else {
          throw new IllegalArgumentException("Invalid value for LINKBAG: " + item);
        }
      }
      setPropertyInternal(key, linkBag);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for LINKBAG: " + entry.getValue());
    }
  }

  private void updateLinkSetFromMapEntry(Entry<String, ?> entry, String key) {
    if (entry.getValue() instanceof Collection<?> collection) {
      var linkSet = new LinkSet(this);
      for (var item : collection) {
        if (item instanceof Identifiable identifiable) {
          linkSet.add(identifiable);
        } else {
          throw new IllegalArgumentException("Invalid value for LINKSET: " + item);
        }
      }
      setPropertyInternal(key, linkSet);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for LINKSET: " + entry.getValue());
    }
  }

  private void updateLinkListFromMapEntry(Entry<String, ?> entry, String key) {
    if (entry.getValue() instanceof Collection<?> collection) {
      var linkList = new LinkList();
      for (var item : collection) {
        if (item instanceof Identifiable identifiable) {
          linkList.add(identifiable);
        } else {
          throw new IllegalArgumentException("Invalid value for LINKLIST: " + item);
        }
      }
      setPropertyInternal(key, linkList);
    } else {
      throw new IllegalArgumentException(
          "Invalid value for LINKLIST: " + entry.getValue());
    }
  }


  public final EntityImpl updateFromJSON(final String iSource, final String iOptions) {
    return super.updateFromJSON(iSource, iOptions);
  }

  /**
   * Writes the field value forcing the type. This method sets the current entity as dirty.
   *
   * <p>if there's a schema definition for the specified field, the value will be converted to
   * respect the schema definition if needed. if the type defined in the schema support less
   * precision than the iPropertyValue provided, the iPropertyValue will be converted following the
   * java casting rules with possible precision loss.
   *
   * @param iFieldName     field name. If contains dots (.) the change is applied to the nested
   *                       documents in chain.
   * @param iPropertyValue field value.
   * @param iFieldType     Forced type (not auto-determined)
   * @return The Record instance itself giving a "fluent interface". Useful to call multiple methods
   * in chain. If the updated entity is another entity (using the dot (.) notation) then the entity
   * returned is the changed one or NULL if no entity has been found in chain
   */
  public EntityImpl field(String iFieldName, Object iPropertyValue, PropertyType... iFieldType) {
    checkForBinding();

    if (iFieldName == null) {
      throw new IllegalArgumentException("Field is null");
    }

    if (iFieldName.isEmpty()) {
      throw new IllegalArgumentException("Field name is empty");
    }

    switch (iFieldName) {
      case EntityHelper.ATTRIBUTE_RID -> {
        recordId.fromString(iPropertyValue.toString());
        return this;
      }
      case EntityHelper.ATTRIBUTE_VERSION -> {
        if (iPropertyValue != null) {
          int v;

          if (iPropertyValue instanceof Number) {
            v = ((Number) iPropertyValue).intValue();
          } else {
            v = Integer.parseInt(iPropertyValue.toString());
          }

          recordVersion = v;
        }
        return this;
      }
    }

    final var lastDotSep = iFieldName.lastIndexOf('.');
    final var lastArraySep = iFieldName.lastIndexOf('[');

    final var lastSep = Math.max(lastArraySep, lastDotSep);
    final var lastIsArray = lastArraySep > lastDotSep;

    if (lastSep > -1) {
      // SUB PROPERTY GET 1 LEVEL BEFORE LAST
      final var subObject = field(iFieldName.substring(0, lastSep));
      if (subObject != null) {
        final var subFieldName =
            lastIsArray ? iFieldName.substring(lastSep) : iFieldName.substring(lastSep + 1);
        if (subObject instanceof EntityImpl) {
          // SUB-ENTITY
          ((EntityImpl) subObject).field(subFieldName, iPropertyValue);
          return (EntityImpl) (((EntityImpl) subObject).isEmbedded() ? this : subObject);
        } else {
          if (subObject instanceof Map<?, ?>) {
            // KEY/VALUE
            ((Map<String, Object>) subObject).put(subFieldName, iPropertyValue);
          } else {
            if (MultiValue.isMultiValue(subObject)) {
              if ((subObject instanceof List<?> || subObject.getClass().isArray()) && lastIsArray) {
                // List // Array Type with a index subscript.
                final var subFieldNameLen = subFieldName.length();

                if (subFieldName.charAt(subFieldNameLen - 1) != ']') {
                  throw new IllegalArgumentException("Missed closing ']'");
                }

                final var indexPart = subFieldName.substring(1, subFieldNameLen - 1);
                final var indexPartObject = EntityHelper.getIndexPart(null, indexPart);
                final var indexAsString =
                    indexPartObject == null ? null : indexPartObject.toString();

                if (indexAsString == null) {
                  throw new IllegalArgumentException(
                      "List / array subscripts must resolve to integer values.");
                }
                try {
                  final var index = Integer.parseInt(indexAsString);
                  MultiValue.setValue(subObject, iPropertyValue, index);
                } catch (NumberFormatException e) {
                  throw new IllegalArgumentException(
                      "List / array subscripts must resolve to integer values.", e);
                }
              } else {
                // APPLY CHANGE TO ALL THE ITEM IN SUB-COLLECTION
                for (var subObjectItem : MultiValue.getMultiValueIterable(subObject)) {
                  if (subObjectItem instanceof EntityImpl) {
                    // SUB-ENTITY, CHECK IF IT'S NOT LINKED
                    if (!((EntityImpl) subObjectItem).isEmbedded()) {
                      throw new IllegalArgumentException(
                          "Property '"
                              + iFieldName
                              + "' points to linked collection of items. You can only change"
                              + " embedded entities in this way");
                    }
                    ((EntityImpl) subObjectItem).field(subFieldName, iPropertyValue);
                  } else {
                    if (subObjectItem instanceof Map<?, ?>) {
                      // KEY/VALUE
                      ((Map<String, Object>) subObjectItem).put(subFieldName, iPropertyValue);
                    }
                  }
                }
              }
              return this;
            }
          }
        }
      } else {
        throw new IllegalArgumentException(
            "Property '"
                + iFieldName.substring(0, lastSep)
                + "' is null, is possible to set a value with dotted notation only on not null"
                + " property");
      }
      return null;
    }

    iFieldName = checkFieldName(iFieldName);

    checkForFields();

    var entry = fields.get(iFieldName);
    final boolean knownProperty;
    final Object oldValue;
    final PropertyType oldType;
    if (entry == null) {
      entry = new EntityEntry();
      fieldSize++;
      fields.put(iFieldName, entry);
      entry.markCreated();
      knownProperty = false;
      oldValue = null;
      oldType = null;
    } else {
      knownProperty = entry.exists();
      oldValue = entry.value;
      oldType = entry.type;
    }

    var fieldType = deriveFieldType(iFieldName, entry,
        iFieldType.length > 0 ? iFieldType[0] : null);
    if (iPropertyValue != null && fieldType != null) {
      iPropertyValue =
          EntityHelper.convertField(getSession(), this, iFieldName, fieldType, null,
              iPropertyValue);
    } else {
      if (iPropertyValue instanceof Enum) {
        iPropertyValue = iPropertyValue.toString();
      }
    }

    if (knownProperty)
    // CHECK IF IS REALLY CHANGED
    {
      if (iPropertyValue == null) {
        if (oldValue == null)
        // BOTH NULL: UNCHANGED
        {
          return this;
        }
      } else {

        try {
          if (iPropertyValue.equals(oldValue)) {
            if (fieldType == oldType) {
              if (!(iPropertyValue instanceof RecordElement))
              // SAME BUT NOT TRACKABLE: SET THE RECORD AS DIRTY TO BE SURE IT'S SAVED
              {
                setDirty();
              }

              // SAVE VALUE: UNCHANGED
              return this;
            }
          } else {
            if (iPropertyValue instanceof byte[]
                && Arrays.equals((byte[]) iPropertyValue, (byte[]) oldValue)) {
              // SAVE VALUE: UNCHANGED
              return this;
            }
          }
        } catch (Exception e) {
          LogManager.instance()
              .warn(
                  this,
                  "Error on checking the value of property %s against the record %s",
                  e,
                  iFieldName,
                  getIdentity());
        }
      }
    }

    if (oldValue instanceof RidBag ridBag) {
      ridBag.setOwner(null);
      ridBag.setRecordAndField(recordId, iFieldName);
    } else {
      if (oldValue instanceof EntityImpl) {
        ((EntityImpl) oldValue).removeOwner(this);
      }
    }

    if (iPropertyValue != null) {
      if (iPropertyValue instanceof EntityImpl) {
        if (PropertyType.EMBEDDED.equals(fieldType)) {
          final var embeddedEntity = (EntityImpl) iPropertyValue;
          EntityInternalUtils.addOwner(embeddedEntity, this);
        } else {
          if (PropertyType.LINK.equals(fieldType)) {
            final var embeddedEntity = (EntityImpl) iPropertyValue;
            EntityInternalUtils.removeOwner(embeddedEntity, this);
          }
        }
      }

      if (iPropertyValue instanceof RidBag ridBag) {
        ridBag.setOwner(
            null); // in order to avoid IllegalStateException when ridBag changes the owner
        // (EntityImpl.merge)
        ridBag.setOwner(this);
        ridBag.setRecordAndField(recordId, iFieldName);
      }
    }

    if (oldType != fieldType) {
      // can be made in a better way, but "keeping type" issue should be solved before
      if (iPropertyValue == null
          || fieldType != null
          || oldType != PropertyType.getTypeByValue(iPropertyValue)) {
        entry.type = fieldType;
      }
    }
    entry.disableTracking(this, oldValue);
    if (iPropertyValue instanceof RID rid) {
      iPropertyValue = refreshNonPersistentRid(rid);
    }

    entry.value = iPropertyValue;
    if (!entry.exists()) {
      entry.setExists(true);
      fieldSize++;
    }
    entry.enableTracking(this);

    if (!entry.isChanged()) {
      entry.original = oldValue;
      entry.markChanged();
    }

    setDirty();

    return this;
  }

  /**
   * Removes a field.
   */
  @Override
  public Object removeField(final String iFieldName) {
    checkForBinding();
    checkForFields();

    if (EntityHelper.ATTRIBUTE_CLASS.equalsIgnoreCase(iFieldName)) {
      throw new UnsupportedOperationException("Cannot remove the class attribute");
    } else {
      if (EntityHelper.ATTRIBUTE_RID.equalsIgnoreCase(iFieldName)) {
        throw new UnsupportedOperationException("Cannot remove the rid of the record");
      }
    }

    final var entry = fields.get(iFieldName);
    if (entry == null) {
      return null;
    }
    var oldValue = entry.value;

    if (entry.exists() && trackingChanges) {
      // SAVE THE OLD VALUE IN A SEPARATE MAP
      if (entry.original == null) {
        entry.original = entry.value;
      }
      entry.value = null;
      entry.setExists(false);
      entry.markChanged();
    } else {
      fields.remove(iFieldName);
    }
    fieldSize--;

    entry.disableTracking(this, oldValue);
    if (oldValue instanceof RidBag) {
      ((RidBag) oldValue).setOwner(null);
    }
    setDirty();
    return oldValue;
  }

  /**
   * Merge current entity with the entity passed as parameter. If the field already exists then the
   * conflicts are managed based on the value of the parameter 'iUpdateOnlyMode'.
   *
   * @param iOther                              Other EntityImpl instance to merge
   * @param iUpdateOnlyMode                     if true, the other entity properties will always be
   *                                            added or overwritten. If false, the missed
   *                                            properties in the "other" entity will be removed by
   *                                            original entity
   * @param iMergeSingleItemsOfMultiValueFields If true, merges single items of multi field fields
   *                                            (collections, maps, arrays, etc)
   */
  public EntityImpl merge(
      final EntityImpl iOther,
      boolean iUpdateOnlyMode,
      boolean iMergeSingleItemsOfMultiValueFields) {
    iOther.checkForBinding();

    checkForBinding();

    iOther.checkForFields();

    var session = getSession();
    if (className == null && iOther.getImmutableSchemaClass() != null) {
      className = iOther.getImmutableSchemaClass().getName(session);
    }

    return mergeMap(
        ((EntityImpl) iOther.getRecord(getSession())).fields,
        iUpdateOnlyMode,
        iMergeSingleItemsOfMultiValueFields);
  }

  /**
   * Returns list of changed fields. There are two types of changes:
   *
   * <ol>
   *   <li>Value of field itself was changed by calling of {@link #field(String, Object)} method for
   *       example.
   *   <li>Internal state of field was changed but was not saved. This case currently is applicable
   *       for for collections only.
   * </ol>
   *
   * @return Array of fields, values of which were changed.
   */
  public String[] getDirtyProperties() {
    checkForBinding();

    if (fields == null || fields.isEmpty()) {
      return EMPTY_STRINGS;
    }

    final Set<String> dirtyFields = new HashSet<>();
    for (var entry : fields.entrySet()) {
      if (entry.getValue().isChanged() || entry.getValue().isTrackedModified()) {
        dirtyFields.add(entry.getKey());
      }
    }
    return dirtyFields.toArray(new String[0]);
  }

  /**
   * Returns the original value of a field before it has been changed.
   *
   * @param iFieldName Property name to retrieve the original value
   */
  public Object getOriginalValue(final String iFieldName) {
    checkForBinding();

    if (fields != null) {
      var entry = fields.get(iFieldName);
      if (entry != null) {
        return entry.original;
      }
    }
    return null;
  }

  public MultiValueChangeTimeLine<Object, Object> getCollectionTimeLine(final String iFieldName) {
    checkForBinding();

    var entry = fields != null ? fields.get(iFieldName) : null;
    return entry != null ? entry.getTimeLine() : null;
  }

  /**
   * Returns the iterator fields
   */
  @Override
  @Nonnull
  public Iterator<Entry<String, Object>> iterator() {
    checkForBinding();
    checkForFields();

    if (fields == null) {
      return EmptyMapEntryIterator.INSTANCE;
    }

    final var iterator = fields.entrySet().iterator();
    return new Iterator<>() {
      private Entry<String, EntityEntry> current;
      private boolean read = true;

      @Override
      public boolean hasNext() {
        while (iterator.hasNext()) {
          current = iterator.next();
          if (current.getValue().exists()
              && (propertyAccess == null || propertyAccess.isReadable(current.getKey()))) {
            read = false;
            return true;
          }
        }
        return false;
      }

      @Override
      public Entry<String, Object> next() {
        if (read) {
          if (!hasNext()) {
            // Look wrong but is correct, it need to fail if there isn't next.
            iterator.next();
          }
        }
        final Entry<String, Object> toRet =
            new Entry<>() {
              private final Entry<String, EntityEntry> intern = current;

              @Override
              public Object setValue(Object value) {
                throw new UnsupportedOperationException();
              }

              @Override
              public Object getValue() {
                return intern.getValue().value;
              }

              @Override
              public String getKey() {
                return intern.getKey();
              }

              @Override
              public int hashCode() {
                return intern.hashCode();
              }

              @Override
              public boolean equals(Object obj) {
                //noinspection rawtypes
                if (obj instanceof Entry entry) {
                  return intern.getKey().equals(entry.getKey())
                      && intern.getValue().value.equals(entry.getValue());
                }

                return intern.equals(obj);
              }
            };
        read = true;
        return toRet;
      }

      @Override
      public void remove() {
        var entry = current.getValue();
        if (trackingChanges) {
          if (entry.isChanged()) {
            entry.original = entry.value;
          }
          entry.value = null;
          entry.setExists(false);
          entry.markChanged();
        } else {
          iterator.remove();
        }
        fieldSize--;

        entry.disableTracking(EntityImpl.this, entry.value);
      }
    };
  }

  /**
   * Checks if a field exists.
   *
   * @return True if exists, otherwise false.
   */
  @Override
  public boolean containsField(final String iFieldName) {
    return hasProperty(iFieldName);
  }

  /**
   * Checks if a property exists.
   *
   * @return True if exists, otherwise false.
   */
  @Override
  public boolean hasProperty(final @Nonnull String propertyName) {
    checkForBinding();

    if (checkForFields(propertyName)
        && (propertyAccess == null || propertyAccess.isReadable(propertyName))) {
      var entry = fields.get(propertyName);
      return entry != null && entry.exists();
    } else {
      return false;
    }
  }

  @Override
  public @Nonnull Result detach() {
    var result = new ResultInternal(null);
    var content = toMap(false);

    for (var entry : content.entrySet()) {
      result.setProperty(entry.getKey(), entry.getValue());
    }

    return result;
  }

  /**
   * Returns true if the record has some owner.
   */
  public boolean hasOwners() {
    return owner != null && owner.get() != null;
  }

  @Override
  public RecordElement getOwner() {
    if (owner == null) {
      return null;
    }
    return owner.get();
  }

  @Deprecated
  public Iterable<RecordElement> getOwners() {
    if (owner == null || owner.get() == null) {
      return Collections.emptyList();
    }

    final List<RecordElement> result = new ArrayList<>();
    result.add(owner.get());
    return result;
  }

  /**
   * Propagates the dirty status to the owner, if any. This happens when the object is embedded in
   * another one.
   */
  @Override
  public void setDirty() {
    // THIS IS IMPORTANT TO BE SURE THAT FIELDS ARE LOADED BEFORE IT'S TOO LATE AND THE RECORD
    // _SOURCE IS NULL
    checkForFields();
    super.setDirty();

    if (owner != null) {
      // PROPAGATES TO THE OWNER
      var ownerEntity = owner.get();

      if (ownerEntity != null) {
        ownerEntity.setDirty();
      }
    }
  }

  @Override
  public void setDirtyNoChanged() {
    if (owner != null) {
      // PROPAGATES TO THE OWNER
      var ownerEntity = owner.get();
      if (ownerEntity != null) {
        ownerEntity.setDirtyNoChanged();
      }
    }

    // THIS IS IMPORTANT TO BE SURE THAT FIELDS ARE LOADED BEFORE IT'S TOO LATE AND THE RECORD
    // _SOURCE IS NULL
    checkForFields();

    super.setDirtyNoChanged();
  }

  @Override
  public final EntityImpl fromStream(final byte[] iRecordBuffer) {
    var session = getSession();
    if (dirty > 0) {
      throw new DatabaseException(session.getDatabaseName(),
          "Cannot call fromStream() on dirty records");
    }

    status = STATUS.UNMARSHALLING;
    try {
      removeAllCollectionChangeListeners();

      fields = null;
      fieldSize = 0;
      contentChanged = false;
      schema = null;

      fetchSchemaIfCan();
      super.fromStream(iRecordBuffer);

      return this;
    } finally {
      status = STATUS.LOADED;
    }
  }

  /**
   * Returns the forced field type if any.
   *
   * @param fieldName name of field to check
   */
  @Nullable
  public PropertyType getPropertyType(final String fieldName) {
    checkForBinding();
    checkForFields(fieldName);

    var entry = fields.get(fieldName);
    if (entry != null) {
      if (propertyAccess == null || propertyAccess.isReadable(fieldName)) {
        return entry.type;
      } else {
        return null;
      }
    }

    return null;
  }

  @Override
  public void unload() {
    if (status == RecordElement.STATUS.NOT_LOADED) {
      return;
    }

    if (dirty > 0) {
      throw new IllegalStateException("Can not unload dirty entity");
    }

    internalReset();

    super.unload();
  }

  /**
   * Clears all the field values and types. Clears only record content, but saves its identity.
   *
   * <p>
   *
   * <p>The following code will clear all data from specified entity. <code>
   * entity.clear(); entity.save(); </code>
   *
   * @see #reset()
   */
  @Override
  public void clear() {
    checkForBinding();

    super.clear();
    internalReset();
    owner = null;
  }

  /**
   * Resets the record values and class type to being reused. It's like you create a EntityImpl from
   * scratch.
   */
  @Override
  public EntityImpl reset() {
    checkForBinding();
    var db = getSession();

    if (db.getTransaction().isActive()) {
      throw new IllegalStateException(
          "Cannot reset entities during a transaction. Create a new one each time");
    }

    super.reset();

    className = null;
    immutableClazz = null;
    immutableSchemaVersion = -1;

    internalReset();

    owner = null;
    return this;
  }

  /**
   * Rollbacks changes to the loaded version without reloading the entity. Works only if tracking
   * changes is enabled @see {@link #isTrackingChanges()} and {@link #setTrackingChanges(boolean)}
   * methods.
   */
  public void undo() {
    if (!trackingChanges) {
      throw new ConfigurationException(
          session.getDatabaseName(),
          "Cannot undo the entity because tracking of changes is disabled");
    }

    if (fields != null) {
      final var vals = fields.entrySet().iterator();
      while (vals.hasNext()) {
        final var next = vals.next();
        final var val = next.getValue();
        if (val.isCreated()) {
          vals.remove();
        } else {
          val.undo();
        }
      }
      fieldSize = fields.size();
    }
  }

  public void undo(final String field) {
    var session = getSession();

    if (!trackingChanges) {
      throw new ConfigurationException(
          session.getDatabaseName(),
          "Cannot undo the entity because tracking of changes is disabled");
    }

    if (fields != null) {
      final var value = fields.get(field);
      if (value != null) {
        if (value.isCreated()) {
          fields.remove(field);
        } else {
          value.undo();
        }
      }
    }

  }

  public boolean isLazyLoad() {
    checkForBinding();

    return lazyLoad;
  }

  public void setLazyLoad(final boolean iLazyLoad) {
    checkForBinding();

    this.lazyLoad = iLazyLoad;
    checkForFields();
  }

  public boolean isTrackingChanges() {
    return trackingChanges;
  }

  /**
   * Enabled or disabled the tracking of changes in the entity. This is needed by some triggers like
   * {@link ClassIndexManager} to determine what fields are changed to update indexes.
   *
   * @param iTrackingChanges True to enable it, otherwise false
   * @return this
   */
  public EntityImpl setTrackingChanges(final boolean iTrackingChanges) {
    checkForBinding();

    this.trackingChanges = iTrackingChanges;
    if (!iTrackingChanges && fields != null) {
      // FREE RESOURCES
      var iter = fields.entrySet().iterator();
      while (iter.hasNext()) {
        var cur = iter.next();
        if (!cur.getValue().exists()) {
          iter.remove();
        } else {
          cur.getValue().clear();
        }
      }
      removeAllCollectionChangeListeners();
    } else {
      addAllMultiValueChangeListeners();
    }
    return this;
  }

  protected void clearTrackData() {
    if (fields != null) {
      // FREE RESOURCES
      for (var cur : fields.entrySet()) {
        if (cur.getValue().exists()) {
          cur.getValue().clear();
          cur.getValue().enableTracking(this);
        } else {
          cur.getValue().clearNotExists();
        }
      }
    }
  }

  void clearTransactionTrackData() {
    if (fields != null) {
      // FREE RESOURCES
      var iter = fields.entrySet().iterator();
      while (iter.hasNext()) {
        var cur = iter.next();
        if (cur.getValue().exists()) {
          cur.getValue().transactionClear();
        } else {
          iter.remove();
        }
      }
    }
  }

  public boolean isOrdered() {
    return ordered;
  }

  public EntityImpl setOrdered(final boolean iOrdered) {
    checkForBinding();

    this.ordered = iOrdered;
    return this;
  }

  @Override
  public boolean equals(Object obj) {
    if (!super.equals(obj)) {
      return false;
    }

    return this == obj || recordId.isValid();
  }

  @Override
  public int hashCode() {
    if (recordId.isValid()) {
      return super.hashCode();
    }

    return System.identityHashCode(this);
  }

  /**
   * Returns the number of fields in memory.
   */
  @Override
  public int fields() {
    checkForBinding();

    checkForFields();
    return fieldSize;
  }

  public boolean isEmpty() {
    checkForBinding();

    checkForFields();
    return fields == null || fields.isEmpty();
  }

  public boolean isEmbedded() {
    return owner != null;
  }

  /**
   * Sets the field type. This overrides the schema property settings if any.
   *
   * @param iFieldName Field name
   * @param iFieldType Type to set between PropertyType enumeration values
   */
  public void setFieldType(final String iFieldName, final PropertyType iFieldType) {
    checkForBinding();

    checkForFields(iFieldName);
    if (iFieldType != null) {
      if (fields == null) {
        fields = ordered ? new LinkedHashMap<>() : new HashMap<>();
      }

      // SET THE FORCED TYPE
      var entry = getOrCreate(iFieldName);
      if (entry.type != iFieldType) {
        if (entry.value == null) {
          entry.type = iFieldType;
        } else {
          field(iFieldName, field(iFieldName), iFieldType);
        }
      }
    } else {
      if (fields != null) {
        // REMOVE THE FIELD TYPE
        var entry = fields.get(iFieldName);
        if (entry != null)
        // EMPTY: OPTIMIZE IT BY REMOVING THE ENTIRE MAP
        {
          entry.type = null;
        }
      }
    }
  }

  /*
   * Initializes the object if has been unserialized
   */
  public boolean deserializeFields(String... iFields) {
    List<String> additional = null;
    if (source == null)
    // ALREADY UNMARSHALLED OR JUST EMPTY
    {
      return true;
    }

    checkForBinding();
    if (iFields != null && iFields.length > 0) {
      // EXTRACT REAL FIELD NAMES
      for (final var f : iFields) {
        if (f != null && !(!f.isEmpty() && f.charAt(0) == '@')) {
          var pos1 = f.indexOf('[');
          var pos2 = f.indexOf('.');
          if (pos1 > -1 || pos2 > -1) {
            var pos = pos1 > -1 ? pos1 : pos2;
            if (pos2 > -1 && pos2 < pos) {
              pos = pos2;
            }

            // REPLACE THE FIELD NAME
            if (additional == null) {
              additional = new ArrayList<>();
            }
            additional.add(f.substring(0, pos));
          }
        }
      }

      if (additional != null) {
        var copy = new String[iFields.length + additional.size()];
        System.arraycopy(iFields, 0, copy, 0, iFields.length);
        var next = iFields.length;
        for (var s : additional) {
          copy[next++] = s;
        }
        iFields = copy;
      }

      // CHECK IF HAS BEEN ALREADY UNMARSHALLED
      if (fields != null && !fields.isEmpty()) {
        var allFound = true;
        for (var f : iFields) {
          if (f != null && !(!f.isEmpty() && f.charAt(0) == '@') && !fields.containsKey(f)) {
            allFound = false;
            break;
          }
        }

        if (allFound)
        // ALL THE REQUESTED FIELDS HAVE BEEN LOADED BEFORE AND AVAILABLE, AVOID UNMARSHALLING
        {
          return true;
        }
      }
    }

    status = RecordElement.STATUS.UNMARSHALLING;
    try {
      recordFormat.fromStream(getSession(), source, this, iFields);
    } finally {
      status = RecordElement.STATUS.LOADED;
    }

    if (iFields != null && iFields.length > 0) {
      for (var field : iFields) {
        if (field != null && !field.isEmpty() && field.charAt(0) == '@')
        // ATTRIBUTE
        {
          return true;
        }
      }

      // PARTIAL UNMARSHALLING
      if (fields != null && !fields.isEmpty()) {
        for (var f : iFields) {
          if (f != null && fields.containsKey(f)) {
            return true;
          }
        }
      }

      // NO FIELDS FOUND
      return false;
    } else {
      if (source != null)
      // FULL UNMARSHALLING
      {
        source = null;
      }
    }

    return true;
  }

  public void setClassNameIfExists(final String iClassName) {
    checkForBinding();

    immutableClazz = null;
    immutableSchemaVersion = -1;

    className = iClassName;

    if (iClassName == null) {
      return;
    }

    var session = getSession();
    final var _clazz = session.getMetadata().getImmutableSchemaSnapshot()
        .getClass(iClassName);
    if (_clazz != null) {
      className = _clazz.getName(session);
      convertFieldsToClass(_clazz);
    }
  }

  @Nullable
  @Override
  public SchemaClass getSchemaClass() {
    checkForBinding();

    if (className == null) {
      fetchClassName();
    }

    if (className == null) {
      return null;
    }

    return getSession().getMetadata().getSchema().getClass(className);
  }

  @Nullable
  public String getSchemaClassName() {
    if (className == null) {
      fetchClassName();
    }

    return className;
  }

  protected void setClassName(@Nullable final String className) {
    checkForBinding();

    immutableClazz = null;
    immutableSchemaVersion = -1;

    this.className = className;

    if (className == null) {
      return;
    }

    var session = getSession();
    var metadata = session.getMetadata();
    this.immutableClazz =
        (SchemaImmutableClass) metadata.getImmutableSchemaSnapshot().getClass(className);
    SchemaClass clazz;
    if (this.immutableClazz != null) {
      clazz = this.immutableClazz;
    } else {
      clazz = metadata.getSchema().getOrCreateClass(className);
    }
    if (clazz != null) {
      this.className = clazz.getName(session);
      convertFieldsToClass(clazz);
    }
  }


  /**
   * Validates the record following the declared constraints defined in schema such as mandatory,
   * notNull, min, max, regexp, etc. If the schema is not defined for the current class or there are
   * no constraints then the validation is ignored.
   *
   * @throws ValidationException if the entity breaks some validation constraints defined in the
   *                             schema
   * @see SchemaProperty
   */
  public void validate() throws ValidationException {
    checkForBinding();

    checkForFields();
    autoConvertValues();

    var session = getSession();

    validateFieldsSecurity(session, this);
    if (!session.isValidationEnabled()) {
      return;
    }

    final var immutableSchemaClass = getImmutableSchemaClass();
    if (immutableSchemaClass != null) {
      if (immutableSchemaClass.isStrictMode(session)) {
        // CHECK IF ALL FIELDS ARE DEFINED
        for (var f : fieldNames()) {
          if (immutableSchemaClass.getProperty(session, f) == null) {
            throw new ValidationException(session.getDatabaseName(),
                "Found additional field '"
                    + f
                    + "'. It cannot be added because the schema class '"
                    + immutableSchemaClass.getName(session)
                    + "' is defined as STRICT");
          }
        }
      }

      final var immutableSchema = session.getMetadata().getImmutableSchemaSnapshot();
      for (var p : immutableSchemaClass.properties(session)) {
        validateField(session, immutableSchema, this, (ImmutableSchemaProperty) p);
      }
    }
  }

  protected String toString(Set<DBRecord> inspected) {
    checkForBinding();

    if (inspected.contains(this)) {
      return "<recursion:rid=" + (recordId != null ? recordId : "null") + ">";
    } else {
      inspected.add(this);
    }

    final var saveDirtyStatus = dirty;
    final var oldUpdateContent = contentChanged;

    try {
      final var buffer = new StringBuilder(128);

      checkForFields();

      var session = getSessionIfDefined();
      if (session != null && !session.isClosed()) {
        final var clsName = getSchemaClassName();
        if (clsName != null) {
          buffer.append(clsName);
        }
      }

      if (recordId != null) {
        if (recordId.isValid()) {
          buffer.append(recordId);
        }
      }

      var first = true;
      for (var f : fields.entrySet()) {
        if (propertyAccess != null && !propertyAccess.isReadable(f.getKey())) {
          continue;
        }
        buffer.append(first ? '{' : ',');
        buffer.append(f.getKey());
        buffer.append(':');
        if (f.getValue().value == null) {
          buffer.append("null");
        } else {
          if (f.getValue().value instanceof Collection<?>
              || f.getValue().value instanceof Map<?, ?>
              || f.getValue().value.getClass().isArray()) {
            buffer.append('[');
            buffer.append(MultiValue.getSize(f.getValue().value));
            buffer.append(']');
          } else {
            if (f.getValue().value instanceof RecordAbstract record) {
              if (record.getIdentity().isValid()) {
                record.getIdentity().toString(buffer);
              } else {
                if (record instanceof EntityImpl) {
                  buffer.append(((EntityImpl) record).toString(inspected));
                } else {
                  buffer.append(record);
                }
              }
            } else {
              buffer.append(f.getValue().value);
            }
          }
        }

        if (first) {
          first = false;
        }
      }
      if (!first) {
        buffer.append('}');
      }

      if (recordId != null && recordId.isValid()) {
        buffer.append(" v");
        buffer.append(recordVersion);
      }

      return buffer.toString();
    } finally {
      dirty = saveDirtyStatus;
      contentChanged = oldUpdateContent;
    }
  }

  private EntityImpl mergeMap(
      final Map<String, EntityEntry> iOther,
      final boolean iUpdateOnlyMode,
      boolean iMergeSingleItemsOfMultiValueFields) {
    checkForFields();
    source = null;

    for (var entry : iOther.entrySet()) {
      var f = entry.getKey();
      var entityEntry = entry.getValue();
      if (!entityEntry.exists()) {
        continue;
      }
      final var otherValue = entityEntry.value;

      var curValue = fields.get(f);

      if (curValue != null && curValue.exists()) {
        final var value = curValue.value;
        if (iMergeSingleItemsOfMultiValueFields) {
          if (value instanceof Map<?, ?>) {
            final var map = (Map<String, Object>) value;
            final var otherMap = (Map<String, Object>) otherValue;

            map.putAll(otherMap);
            continue;
          } else {
            if (MultiValue.isMultiValue(value) && !(value instanceof RidBag)) {
              for (var item : MultiValue.getMultiValueIterable(otherValue)) {
                if (!MultiValue.contains(value, item)) {
                  MultiValue.add(value, item);
                }
              }
              continue;
            }
          }
        }
        var bagsMerged = false;
        if (value instanceof RidBag && otherValue instanceof RidBag) {
          bagsMerged =
              ((RidBag) value).tryMerge((RidBag) otherValue, iMergeSingleItemsOfMultiValueFields);
        }

        if (!bagsMerged && (value != null && !value.equals(otherValue))
            || (value == null && otherValue != null)) {
          setPropertyInternal(f, otherValue);
        }
      } else {
        setPropertyInternal(f, otherValue);
      }
    }

    if (!iUpdateOnlyMode) {
      // REMOVE PROPERTIES NOT FOUND IN OTHER ENTITY
      for (var f : getPropertyNamesInternal()) {
        if (!iOther.containsKey(f) || !iOther.get(f).exists()) {
          removePropertyInternal(f);
        }
      }
    }

    return this;
  }

  @Override
  protected final RecordAbstract fill(
      final RID iRid, final int iVersion, final byte[] iBuffer, final boolean iDirty) {
    var session = getSession();
    if (dirty > 0) {
      throw new DatabaseException(session.getDatabaseName(), "Cannot call fill() on dirty records");
    }

    schema = null;
    fetchSchemaIfCan();
    return super.fill(iRid, iVersion, iBuffer, iDirty);
  }

  @Override
  protected void clearSource() {
    super.clearSource();
    schema = null;
  }

  protected GlobalProperty getGlobalPropertyById(int id) {
    checkForBinding();
    var session = getSession();
    if (schema == null) {
      var metadata = session.getMetadata();
      schema = metadata.getImmutableSchemaSnapshot();
    }
    var prop = schema.getGlobalPropertyById(id);
    if (prop == null) {
      if (session.isClosed()) {
        throw new DatabaseException(session.getDatabaseName(),
            "Cannot unmarshall the entity because no database is active, use detach for use the"
                + " entity outside the database session scope");
      }

      var metadata = session.getMetadata();
      if (metadata.getImmutableSchemaSnapshot() != null) {
        metadata.clearThreadLocalSchemaSnapshot();
      }
      metadata.reload();
      metadata.makeThreadLocalSchemaSnapshot();
      schema = metadata.getImmutableSchemaSnapshot();
      prop = schema.getGlobalPropertyById(id);
    }
    return prop;
  }

  void fillClassIfNeed(final String iClassName) {
    checkForBinding();

    if (this.className == null) {
      immutableClazz = null;
      immutableSchemaVersion = -1;
      className = iClassName;
    }
  }

  protected SchemaImmutableClass getImmutableSchemaClass() {
    return getImmutableSchemaClass(getSessionIfDefined());
  }

  protected SchemaImmutableClass getImmutableSchemaClass(
      @Nullable DatabaseSessionInternal database) {
    if (immutableClazz == null) {
      if (className == null) {
        fetchClassName();
      }

      if (className != null) {
        if (database != null && !database.isClosed()) {
          final Schema immutableSchema = database.getMetadata().getImmutableSchemaSnapshot();
          if (immutableSchema == null) {
            return null;
          }
          //noinspection deprecation
          immutableSchemaVersion = immutableSchema.getVersion();
          immutableClazz = (SchemaImmutableClass) immutableSchema.getClass(className);
        }
      }
    }

    return immutableClazz;
  }

  protected void rawField(
      final String iFieldName, final Object iFieldValue, final PropertyType iFieldType) {
    checkForBinding();

    if (fields == null) {
      fields = ordered ? new LinkedHashMap<>() : new HashMap<>();
    }

    var entry = getOrCreate(iFieldName);
    entry.disableTracking(this, entry.value);

    if (iFieldValue instanceof RID rid && !rid.isPersistent()) {
      entry.value = refreshNonPersistentRid(rid);
    } else {
      entry.value = iFieldValue;
    }

    entry.type = iFieldType;
    entry.enableTracking(this);
    if (iFieldValue instanceof RidBag) {
      ((RidBag) iFieldValue).setRecordAndField(recordId, iFieldName);
    }
  }

  private EntityEntry getOrCreate(String key) {
    var entry = fields.get(key);
    if (entry == null) {
      entry = new EntityEntry();
      fieldSize++;
      fields.put(key, entry);
    }
    return entry;
  }

  boolean rawContainsField(final String iFiledName) {
    checkForBinding();
    return fields != null && fields.containsKey(iFiledName);
  }

  public void autoConvertValues() {
    checkForBinding();

    var session = getSession();
    SchemaClass clazz = getImmutableSchemaClass();
    if (clazz != null) {
      for (var prop : clazz.properties(session)) {
        var type = prop.getType(session);
        var linkedType = prop.getLinkedType(session);
        var linkedClass = prop.getLinkedClass(session);
        if (type == PropertyType.EMBEDDED && linkedClass != null) {
          convertToEmbeddedType(session, prop);
          continue;
        }
        if (fields == null) {
          continue;
        }
        final var entry = fields.get(prop.getName(session));
        if (entry == null) {
          continue;
        }
        if (!entry.isCreated() && !entry.isChanged()) {
          continue;
        }
        var value = entry.value;
        if (value == null) {
          continue;
        }
        try {
          if (type == PropertyType.LINKBAG
              && !(entry.value instanceof RidBag)
              && entry.value instanceof Collection) {
            var newValue = new RidBag(session);
            newValue.setRecordAndField(recordId, prop.getName(session));
            for (var o : ((Collection<Object>) entry.value)) {
              if (!(o instanceof Identifiable identifiable)) {
                throw new ValidationException(session.getDatabaseName(),
                    "Invalid value in ridbag: " + o);
              }
              newValue.add(identifiable.getIdentity());
            }
            entry.value = newValue;
          }
          if (type == PropertyType.LINKMAP) {
            if (entry.value instanceof Map) {
              var map = (Map<String, Object>) entry.value;
              var newMap = new LinkMap(this);
              var changed = false;
              for (var stringObjectEntry : map.entrySet()) {
                var val = stringObjectEntry.getValue();
                if (MultiValue.isMultiValue(val) && MultiValue.getSize(val) == 1) {
                  val = MultiValue.getFirstValue(val);
                  if (val instanceof Result) {
                    val = ((Result) val).getIdentity();
                  }
                  changed = true;
                }
                newMap.put(stringObjectEntry.getKey(), (Identifiable) val);
              }
              if (changed) {
                entry.value = newMap;
              }
            }
          }

          if (linkedType == null) {
            continue;
          }

          if (type == PropertyType.EMBEDDEDLIST) {
            var list = new TrackedList<Object>(this);
            var values = (Collection<Object>) value;
            for (var object : values) {
              list.add(PropertyType.convert(session, object, linkedType.getDefaultJavaType()));
            }
            entry.value = list;
            replaceListenerOnAutoconvert(entry);
          } else {
            if (type == PropertyType.EMBEDDEDMAP) {
              Map<String, Object> map = new TrackedMap<>(this);
              var values = (Map<String, Object>) value;
              for (var object : values.entrySet()) {
                map.put(
                    object.getKey(),
                    PropertyType.convert(session, object.getValue(),
                        linkedType.getDefaultJavaType()));
              }
              entry.value = map;
              replaceListenerOnAutoconvert(entry);
            } else {
              if (type == PropertyType.EMBEDDEDSET) {
                Set<Object> set = new TrackedSet<>(this);
                var values = (Collection<Object>) value;
                for (var object : values) {
                  set.add(PropertyType.convert(session, object, linkedType.getDefaultJavaType()));
                }
                entry.value = set;
                replaceListenerOnAutoconvert(entry);
              }
            }
          }
        } catch (Exception e) {
          throw BaseException.wrapException(
              new ValidationException(session.getDatabaseName(),
                  "impossible to convert value of field \"" + prop.getName(session) + "\""),
              e, session.getDatabaseName());
        }
      }
    }
  }

  private void convertToEmbeddedType(DatabaseSessionInternal session, SchemaProperty prop) {
    final var entry = fields.get(prop.getName(session));
    var linkedClass = prop.getLinkedClass(session);
    if (entry == null || linkedClass == null) {
      return;
    }
    if (!entry.isCreated() && !entry.isChanged()) {
      return;
    }
    var value = entry.value;
    if (value == null) {
      return;
    }
    try {
      if (value instanceof EntityImpl) {
        SchemaClass entityClass = ((EntityImpl) value).getImmutableSchemaClass();
        if (entityClass == null) {
          ((EntityImpl) value).setClass(linkedClass);
        } else {
          if (!entityClass.isSubClassOf(session, linkedClass)) {
            throw new ValidationException(session.getDatabaseName(),
                "impossible to convert value of field \""
                    + prop.getName(session)
                    + "\", incompatible with "
                    + linkedClass);
          }
        }
      } else {
        if (value instanceof Map) {
          entry.disableTracking(this, value);
          var newValue = new EntityImpl(getSession(), linkedClass);
          //noinspection rawtypes
          newValue.updateFromMap((Map) value);
          entry.value = newValue;
          newValue.addOwner(this);
        } else {
          throw new ValidationException(session.getDatabaseName(),
              "impossible to convert value of field \"" + prop.getName(session) + "\"");
        }
      }

    } catch (Exception e) {
      throw BaseException.wrapException(
          new ValidationException(session.getDatabaseName(),
              "impossible to convert value of field \"" + prop.getName(session) + "\""),
          e, session.getDatabaseName());
    }
  }

  private void replaceListenerOnAutoconvert(final EntityEntry entry) {
    entry.replaceListener(this);
  }

  /**
   * Internal.
   */
  @Override
  public byte getRecordType() {
    return RECORD_TYPE;
  }

  /**
   * Internal.
   */
  protected void addOwner(final RecordElement iOwner) {
    checkForBinding();

    if (iOwner == null) {
      return;
    }

    if (recordId.isPersistent()) {
      throw new DatabaseException(session.getDatabaseName(),
          "Cannot add owner to a persistent entity");
    }

    this.owner = new WeakReference<>(iOwner);

    var tx = getSession().getTransaction();
    if (!tx.isActive()) {
      return;
    }

    var optimistic = (FrontendTransactionOptimistic) tx;
    optimistic.deleteRecordOperation(this);

    recordId.reset();
    if (Entity.DEFAULT_CLASS_NAME.equals(className)) {
      setClassName(null);
    }
  }

  void removeOwner(final RecordElement iRecordElement) {
    if (owner != null && owner.get() == iRecordElement) {
      assert !recordId.isPersistent();
      owner = null;
    }
  }

  void convertAllMultiValuesToTrackedVersions() {
    checkForBinding();

    if (fields == null) {
      return;
    }

    var session = getSession();
    for (var fieldEntry : fields.entrySet()) {
      var entry = fieldEntry.getValue();
      final var fieldValue = entry.value;
      if (fieldValue instanceof RidBag) {
        if (isEmbedded()) {
          throw new DatabaseException(session.getDatabaseName(),
              "RidBag are supported only at entity root");
        }
        ((RidBag) fieldValue).checkAndConvert();
      }
      if (!(fieldValue instanceof Collection<?>)
          && !(fieldValue instanceof Map<?, ?>)
          && !(fieldValue instanceof EntityImpl)) {
        continue;
      }
      if (entry.enableTracking(this)) {
        if (entry.getTimeLine() != null
            && !entry.getTimeLine().getMultiValueChangeEvents().isEmpty()) {
          //noinspection rawtypes
          checkTimelineTrackable(entry.getTimeLine(), (TrackedMultiValue) entry.value);
        }
        continue;
      }

      if (fieldValue instanceof EntityImpl && ((EntityImpl) fieldValue).isEmbedded()) {
        ((EntityImpl) fieldValue).convertAllMultiValuesToTrackedVersions();
        continue;
      }

      var fieldType = entry.type;
      if (fieldType == null) {
        SchemaClass clazz = getImmutableSchemaClass();
        if (clazz != null) {
          final var prop = clazz.getProperty(session, fieldEntry.getKey());
          fieldType = prop != null ? prop.getType(session) : null;
        }
      }
      if (fieldType == null) {
        fieldType = PropertyType.getTypeByValue(fieldValue);
      }

      RecordElement newValue = null;
      switch (fieldType) {
        case EMBEDDEDLIST:
          if (fieldValue instanceof List<?>) {
            newValue = new TrackedList<>(this);
            fillTrackedCollection(
                (Collection<Object>) newValue, newValue, (Collection<Object>) fieldValue);
          }
          break;
        case EMBEDDEDSET:
          if (fieldValue instanceof Set<?>) {
            newValue = new TrackedSet<>(this);
            fillTrackedCollection(
                (Collection<Object>) newValue, newValue, (Collection<Object>) fieldValue);
          }
          break;
        case EMBEDDEDMAP:
          if (fieldValue instanceof Map<?, ?>) {
            newValue = new TrackedMap<>(this);
            fillTrackedMap(
                (Map<String, Object>) newValue, newValue, (Map<String, Object>) fieldValue);
          }
          break;
        case LINKLIST:
          if (fieldValue instanceof List<?>) {
            newValue = new LinkList(this, (Collection<Identifiable>) fieldValue);
          }
          break;
        case LINKSET:
          if (fieldValue instanceof Set<?>) {
            newValue = new LinkSet(this, (Collection<Identifiable>) fieldValue);
          }
          break;
        case LINKMAP:
          if (fieldValue instanceof Map<?, ?>) {
            newValue = new LinkMap(this, (Map<String, Identifiable>) fieldValue);
          }
          break;
        case LINKBAG:
          if (fieldValue instanceof Collection<?>) {
            var bag = new RidBag(session);
            bag.setOwner(this);
            bag.setRecordAndField(recordId, fieldEntry.getKey());
            for (var item : (Collection<Identifiable>) fieldValue) {
              bag.add(item.getIdentity());
            }
            newValue = bag;
          }
          break;
        default:
          break;
      }

      if (newValue != null) {
        entry.enableTracking(this);
        entry.value = newValue;
        if (fieldType == PropertyType.LINKSET || fieldType == PropertyType.LINKLIST) {
          for (var rec : (Collection<Identifiable>) newValue) {
            if (rec instanceof EntityImpl) {
              ((EntityImpl) rec).convertAllMultiValuesToTrackedVersions();
            }
          }
        } else {
          if (fieldType == PropertyType.LINKMAP) {
            for (var rec : (Collection<Identifiable>) ((Map<?, ?>) newValue).values()) {
              if (rec instanceof EntityImpl) {
                ((EntityImpl) rec).convertAllMultiValuesToTrackedVersions();
              }
            }
          }
        }
      }
    }
  }

  private void checkTimelineTrackable(
      MultiValueChangeTimeLine<Object, Object> timeLine,
      TrackedMultiValue<Object, Object> origin) {
    var events = timeLine.getMultiValueChangeEvents();
    for (var event : events) {
      var value = event.getValue();
      if (event.getChangeType() == ChangeType.ADD
          && !(value instanceof TrackedMultiValue)) {
        if (value instanceof List) {
          var newCollection = new TrackedList<>(this);
          fillTrackedCollection(newCollection, newCollection, (Collection<Object>) value);
          origin.replace(event, newCollection);
        } else {
          if (value instanceof Set) {
            var newCollection = new TrackedSet<>(this);
            fillTrackedCollection(newCollection, newCollection, (Collection<Object>) value);
            origin.replace(event, newCollection);

          } else {
            if (value instanceof Map) {
              var newMap = new TrackedMap<Object>(this);
              fillTrackedMap(newMap, newMap, (Map<String, Object>) value);
              origin.replace(event, newMap);
            }
          }
        }
      }
    }
  }

  private void fillTrackedCollection(
      Collection<Object> dest, RecordElement parent, Collection<Object> source) {
    var session = getSession();
    for (var cur : source) {
      if (cur instanceof EntityImpl) {
        ((EntityImpl) cur).addOwner((RecordElement) dest);
        ((EntityImpl) cur).convertAllMultiValuesToTrackedVersions();
        ((EntityImpl) cur).clearTrackData();
      } else {
        if (cur instanceof List) {
          @SuppressWarnings("rawtypes")
          TrackedList newList = new TrackedList<>(parent);
          fillTrackedCollection(newList, newList, (Collection<Object>) cur);
          cur = newList;
        } else {
          if (cur instanceof Set) {
            var newSet = new TrackedSet<Object>(parent);
            fillTrackedCollection(newSet, newSet, (Collection<Object>) cur);
            cur = newSet;
          } else {
            if (cur instanceof Map) {
              var newMap = new TrackedMap<Object>(parent);
              fillTrackedMap(newMap, newMap, (Map<String, Object>) cur);
              cur = newMap;
            } else {
              if (cur instanceof RidBag) {
                throw new DatabaseException(session.getDatabaseName(),
                    "RidBag are supported only at entity root");
              }
            }
          }
        }
      }

      dest.add(cur);
    }
  }

  private void fillTrackedMap(
      Map<String, Object> dest, RecordElement parent, Map<String, Object> source) {
    for (var cur : source.entrySet()) {
      var value = cur.getValue();
      if (value instanceof EntityImpl) {
        ((EntityImpl) value).convertAllMultiValuesToTrackedVersions();
        ((EntityImpl) value).clearTrackData();
      } else {
        if (cur.getValue() instanceof List) {
          var newList = new TrackedList<Object>(parent);
          fillTrackedCollection(newList, newList, (Collection<Object>) value);
          value = newList;
        } else {
          if (value instanceof Set) {
            var newSet = new TrackedSet<Object>(parent);
            fillTrackedCollection(newSet, newSet, (Collection<Object>) value);
            value = newSet;
          } else {
            if (value instanceof Map) {
              var newMap = new TrackedMap<Object>(parent);
              fillTrackedMap(newMap, newMap, (Map<String, Object>) value);
              value = newMap;
            } else {
              if (value instanceof RidBag) {
                throw new DatabaseException(session.getDatabaseName(),
                    "RidBag are supported only at entity root");
              }
            }
          }
        }
      }
      dest.put(cur.getKey(), value);
    }
  }

  private void internalReset() {
    removeAllCollectionChangeListeners();
    if (fields != null) {
      fields.clear();
    }
    fieldSize = 0;
  }

  boolean checkForFields(final String... iFields) {
    if (fields == null) {
      fields = ordered ? new LinkedHashMap<>() : new HashMap<>();
    }

    if (source != null) {
      checkForBinding();

      if (status == RecordElement.STATUS.LOADED) {
        return deserializeFields(iFields);
      }
    }

    return true;
  }

  Object accessProperty(final String property) {
    checkForBinding();

    if (checkForFields(property)) {
      if (propertyAccess == null || propertyAccess.isReadable(property)) {
        var entry = fields.get(property);
        if (entry != null) {
          return entry.value;
        } else {
          return null;
        }
      } else {
        return null;
      }
    } else {
      return null;
    }
  }

  private void setup() {
    if (session != null) {
      recordFormat = session.getSerializer();
    }

    if (recordFormat == null) {
      recordFormat = DatabaseSessionAbstract.getDefaultSerializer();
    }
  }

  private static String checkFieldName(final String iFieldName) {
    final var c = SchemaShared.checkFieldNameIfValid(iFieldName);
    if (c != null) {
      throw new IllegalArgumentException(
          "Invalid field name '" + iFieldName + "'. Character '" + c + "' is invalid");
    }

    return iFieldName;
  }

  void setClass(final SchemaClass iClass) {
    checkForBinding();

    var session = getSession();
    if (iClass != null && iClass.isAbstract(session)) {
      throw new SchemaException(session.getDatabaseName(),
          "Cannot create a entity of the abstract class '" + iClass + "'");
    }

    if (recordId.isPersistent()) {
      throw new UnsupportedOperationException("Cannot change class of persistent record");
    }

    if (iClass == null) {
      className = null;
    } else {
      className = iClass.getName(session);
    }

    immutableClazz = null;
    immutableSchemaVersion = -1;
    if (iClass != null) {
      convertFieldsToClass(iClass);
    }
  }

  Set<Entry<String, EntityEntry>> getRawEntries() {
    checkForBinding();

    checkForFields();
    return fields == null ? new HashSet<>() : fields.entrySet();
  }

  List<Entry<String, EntityEntry>> getFilteredEntries() {
    checkForBinding();
    checkForFields();

    if (fields == null) {
      return Collections.emptyList();
    } else {
      if (propertyAccess == null) {
        return fields.entrySet().stream()
            .filter((x) -> x.getValue().exists())
            .collect(Collectors.toList());
      } else {
        return fields.entrySet().stream()
            .filter((x) -> x.getValue().exists() && propertyAccess.isReadable(x.getKey()))
            .collect(Collectors.toList());
      }
    }
  }

  private void fetchSchemaIfCan() {
    if (schema == null) {
      var db = getSession();
      if (!db.isClosed()) {
        var metadata = db.getMetadata();
        schema = metadata.getImmutableSchemaSnapshot();
      }
    }
  }

  private void fetchClassName() {
    final var database = getSession();

    if (!database.isClosed()) {
      if (recordId != null) {
        if (recordId.getClusterId() >= 0) {
          final Schema schema = database.getMetadata().getImmutableSchemaSnapshot();
          if (schema != null) {
            var clazz = schema.getClassByClusterId(recordId.getClusterId());
            if (clazz != null) {
              className = clazz.getName(database);
            }
          }
        }
      }
    }
  }

  void autoConvertFieldsToClass(final DatabaseSessionInternal database) {
    checkForBinding();

    if (className != null) {
      var klazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(className);
      if (klazz != null) {
        convertFieldsToClass(klazz);
      }
    }
  }

  /**
   * Checks and convert the field of the entity matching the types specified by the class.
   */
  private void convertFieldsToClass(final SchemaClass clazz) {
    var session = getSession();

    for (var prop : clazz.properties(session)) {
      var entry = fields != null ? fields.get(prop.getName(session)) : null;
      if (entry != null && entry.exists()) {
        if (entry.type == null || entry.type != prop.getType(session)) {
          var preChanged = entry.isChanged();
          var preCreated = entry.isCreated();
          field(prop.getName(session), entry.value, prop.getType(session));
          if (recordId.isNew()) {
            if (preChanged) {
              entry.markChanged();
            } else {
              entry.unmarkChanged();
            }
            if (preCreated) {
              entry.markCreated();
            } else {
              entry.unmarkCreated();
            }
          }
        }
      } else {
        var defValue = prop.getDefaultValue(session);
        if (defValue != null && /*defValue.length() > 0 && */ !containsField(
            prop.getName(session))) {
          var curFieldValue = SQLHelper.parseDefaultValue(session, this, defValue);
          var fieldValue =
              EntityHelper.convertField(session,
                  this, prop.getName(session), prop.getType(session), null, curFieldValue);
          rawField(prop.getName(session), fieldValue, prop.getType(session));
        }
      }
    }
  }

  private PropertyType deriveFieldType(String iFieldName, EntityEntry entry,
      PropertyType fieldType) {
    SchemaClass clazz = getImmutableSchemaClass();
    var session = getSession();

    if (clazz != null) {
      // SCHEMA-FULL?
      final var prop = clazz.getProperty(session, iFieldName);
      if (prop != null) {
        entry.property = prop;
        fieldType = prop.getType(session);
        if (fieldType != PropertyType.ANY) {
          entry.type = fieldType;
        }
      }
    }

    return fieldType;
  }

  private RID refreshNonPersistentRid(RID identifiable) {
    if (!identifiable.isPersistent()) {
      identifiable = session.refreshRid(identifiable);
    }

    return identifiable;
  }

  private boolean assertIfAlreadyLoaded(RID rid) {
    var session = getSession();

    var tx = session.getTransaction();
    if (tx.isActive()) {
      var txEntry = tx.getRecordEntry(rid);
      if (txEntry != null) {
        if (txEntry.type == RecordOperation.DELETED) {
          throw new DatabaseException("Record with rid : " + rid + " is already deleted");
        } else {
          throw new DatabaseException("Record with rid : " + rid + " is already created");
        }
      }
    }

    var localCache = session.getLocalCache();
    if (localCache.findRecord(rid) != null) {
      throw new DatabaseException("Instance of record with rid : " + rid + " is already created");
    }

    return true;
  }

  private void removeAllCollectionChangeListeners() {
    if (fields == null) {
      return;
    }

    for (final var field : fields.entrySet()) {
      var entityEntry = field.getValue();

      var value = entityEntry.value;
      entityEntry.disableTracking(this, value);
    }
  }

  private void addAllMultiValueChangeListeners() {
    if (fields == null) {
      return;
    }

    for (final var field : fields.entrySet()) {
      field.getValue().enableTracking(this);
    }
  }

  void checkClass(DatabaseSessionInternal database) {
    checkForBinding();
    if (className == null) {
      fetchClassName();
    }

    final Schema immutableSchema = database.getMetadata().getImmutableSchemaSnapshot();
    if (immutableSchema == null) {
      return;
    }

    if (immutableClazz == null) {
      //noinspection deprecation
      immutableSchemaVersion = immutableSchema.getVersion();
      immutableClazz = (SchemaImmutableClass) immutableSchema.getClass(className);
    } else {
      //noinspection deprecation
      if (immutableSchemaVersion < immutableSchema.getVersion()) {
        //noinspection deprecation
        immutableSchemaVersion = immutableSchema.getVersion();
        immutableClazz = (SchemaImmutableClass) immutableSchema.getClass(className);
      }
    }
  }

  ImmutableSchema getImmutableSchema() {
    return schema;
  }

  void checkEmbeddable() {
    var session = getSession();

    if (isVertex() || isStatefulEdge()) {
      throw new DatabaseException(session.getDatabaseName(),
          "Vertices or Edges cannot be stored as embedded");
    }
  }
}
