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
package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.schema.Collate;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.MultiValueChangeEvent;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.CommandExecutorSQLCreateIndex;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * Index that consist of several indexDefinitions like {@link PropertyIndexDefinition}.
 */
public class CompositeIndexDefinition extends AbstractIndexDefinition {

  private final List<IndexDefinition> indexDefinitions;
  private String className;
  private int multiValueDefinitionIndex = -1;
  private CompositeCollate collate = new CompositeCollate(this);

  public CompositeIndexDefinition() {
    indexDefinitions = new ArrayList<>(5);
  }

  /**
   * Constructor for new index creation.
   *
   * @param iClassName - name of class which is owner of this index
   */
  public CompositeIndexDefinition(final String iClassName) {
    super();

    indexDefinitions = new ArrayList<>(5);
    className = iClassName;
  }

  /**
   * Constructor for new index creation.
   *
   * @param iClassName - name of class which is owner of this index
   * @param iIndexes   List of indexDefinitions to add in given index.
   */
  public CompositeIndexDefinition(
      final String iClassName, final List<? extends IndexDefinition> iIndexes) {
    super();

    indexDefinitions = new ArrayList<>(5);
    for (var indexDefinition : iIndexes) {
      indexDefinitions.add(indexDefinition);
      collate.addCollate(indexDefinition.getCollate());

      if (indexDefinition instanceof IndexDefinitionMultiValue) {
        if (multiValueDefinitionIndex == -1) {
          multiValueDefinitionIndex = indexDefinitions.size() - 1;
        } else {
          throw new IndexException("Composite key cannot contain more than one collection item");
        }
      }
    }

    className = iClassName;
  }

  /**
   * {@inheritDoc}
   */
  public String getClassName() {
    return className;
  }

  /**
   * Add new indexDefinition in current composite.
   *
   * @param indexDefinition Index to add.
   */
  public void addIndex(final IndexDefinition indexDefinition) {
    indexDefinitions.add(indexDefinition);
    if (indexDefinition instanceof IndexDefinitionMultiValue) {
      if (multiValueDefinitionIndex == -1) {
        multiValueDefinitionIndex = indexDefinitions.size() - 1;
      } else {
        throw new IndexException("Composite key cannot contain more than one collection item");
      }
    }

    collate.addCollate(indexDefinition.getCollate());
  }

  /**
   * {@inheritDoc}
   */
  public List<String> getFields() {
    final List<String> fields = new LinkedList<>();
    for (final var indexDefinition : indexDefinitions) {
      fields.addAll(indexDefinition.getFields());
    }
    return Collections.unmodifiableList(fields);
  }

  /**
   * {@inheritDoc}
   */
  public List<String> getFieldsToIndex() {
    final List<String> fields = new LinkedList<>();
    for (final var indexDefinition : indexDefinitions) {
      fields.addAll(indexDefinition.getFieldsToIndex());
    }
    return Collections.unmodifiableList(fields);
  }

  /**
   * {@inheritDoc}
   */
  public Object getDocumentValueToIndex(
      DatabaseSessionInternal session, final EntityImpl entity) {
    final List<CompositeKey> compositeKeys = new ArrayList<>(10);
    final var firstKey = new CompositeKey();
    var containsCollection = false;

    compositeKeys.add(firstKey);

    for (final var indexDefinition : indexDefinitions) {
      final var result = indexDefinition.getDocumentValueToIndex(session, entity);

      if (result == null && isNullValuesIgnored()) {
        return null;
      }

      // for empty collections we add null key in index
      if (result instanceof Collection
          && ((Collection<?>) result).isEmpty()
          && isNullValuesIgnored()) {
        return null;
      }

      containsCollection = addKey(firstKey, compositeKeys, containsCollection, result);
    }

    if (!containsCollection) {
      return firstKey;
    }

    return compositeKeys;
  }

  public int getMultiValueDefinitionIndex() {
    return multiValueDefinitionIndex;
  }

  public String getMultiValueField() {
    if (multiValueDefinitionIndex >= 0) {
      return indexDefinitions.get(multiValueDefinitionIndex).getFields().get(0);
    }

    return null;
  }

  /**
   * {@inheritDoc}
   */
  public Object createValue(DatabaseSessionInternal session, final List<?> params) {
    var currentParamIndex = 0;
    final var firstKey = new CompositeKey();

    final List<CompositeKey> compositeKeys = new ArrayList<>(10);
    compositeKeys.add(firstKey);

    var containsCollection = false;

    for (final var indexDefinition : indexDefinitions) {
      if (currentParamIndex + 1 > params.size()) {
        break;
      }

      final var endIndex =
          Math.min(currentParamIndex + indexDefinition.getParamCount(), params.size());
      final var indexParams = params.subList(currentParamIndex, endIndex);
      currentParamIndex += indexDefinition.getParamCount();

      final var keyValue = indexDefinition.createValue(session, indexParams);

      if (keyValue == null && isNullValuesIgnored()) {
        return null;
      }

      // for empty collections we add null key in index
      if (keyValue instanceof Collection
          && ((Collection<?>) keyValue).isEmpty()
          && isNullValuesIgnored()) {
        return null;
      }

      containsCollection = addKey(firstKey, compositeKeys, containsCollection, keyValue);
    }

    if (!containsCollection) {
      return firstKey;
    }

    return compositeKeys;
  }

  public IndexDefinitionMultiValue getMultiValueDefinition() {
    if (multiValueDefinitionIndex > -1) {
      return (IndexDefinitionMultiValue) indexDefinitions.get(multiValueDefinitionIndex);
    }

    return null;
  }

  public CompositeKey createSingleValue(DatabaseSessionInternal session, final List<?> params) {
    final var compositeKey = new CompositeKey();
    var currentParamIndex = 0;

    for (final var indexDefinition : indexDefinitions) {
      if (currentParamIndex + 1 > params.size()) {
        break;
      }

      final var endIndex =
          Math.min(currentParamIndex + indexDefinition.getParamCount(), params.size());

      final var indexParams = params.subList(currentParamIndex, endIndex);
      currentParamIndex += indexDefinition.getParamCount();

      final Object keyValue;

      if (indexDefinition instanceof IndexDefinitionMultiValue) {
        keyValue =
            ((IndexDefinitionMultiValue) indexDefinition)
                .createSingleValue(session, indexParams.toArray());
      } else {
        keyValue = indexDefinition.createValue(session, indexParams);
      }

      if (keyValue == null && isNullValuesIgnored()) {
        return null;
      }

      compositeKey.addKey(keyValue);
    }

    return compositeKey;
  }

  private static boolean addKey(
      CompositeKey firstKey,
      List<CompositeKey> compositeKeys,
      boolean containsCollection,
      Object keyValue) {
    // in case of collection we split single composite key on several composite keys
    // each of those composite keys contain single collection item.
    // we can not contain more than single collection item in index
    if (keyValue instanceof Collection<?> collectionKey) {
      final int collectionSize;

      // we insert null if collection is empty
      if (collectionKey.isEmpty()) {
        collectionSize = 1;
      } else {
        collectionSize = collectionKey.size();
      }

      // if that is first collection we split single composite key on several keys, each of those
      // composite keys contain single item from collection
      if (!containsCollection)
      // sure we need to expand collection only if collection size more than one, otherwise
      // collection of composite keys already contains original composite key
      {
        for (var i = 1; i < collectionSize; i++) {
          final var compositeKey = new CompositeKey(firstKey.getKeys());
          compositeKeys.add(compositeKey);
        }
      } else {
        throw new IndexException("Composite key cannot contain more than one collection item");
      }

      var compositeIndex = 0;
      if (!collectionKey.isEmpty()) {
        for (final var keyItem : collectionKey) {
          final var compositeKey = compositeKeys.get(compositeIndex);
          compositeKey.addKey(keyItem);

          compositeIndex++;
        }
      } else {
        firstKey.addKey(null);
      }

      containsCollection = true;
    } else if (containsCollection) {
      for (final var compositeKey : compositeKeys) {
        compositeKey.addKey(keyValue);
      }
    } else {
      firstKey.addKey(keyValue);
    }

    return containsCollection;
  }

  /**
   * {@inheritDoc}
   */
  public Object createValue(DatabaseSessionInternal session, final Object... params) {
    if (params.length == 1 && params[0] instanceof Collection) {
      return params[0];
    }

    return createValue(session, Arrays.asList(params));
  }

  public void processChangeEvent(
      DatabaseSessionInternal session,
      MultiValueChangeEvent<?, ?> changeEvent,
      Object2IntOpenHashMap<CompositeKey> keysToAdd,
      Object2IntOpenHashMap<CompositeKey> keysToRemove,
      Object... params) {

    final var indexDefinitionMultiValue =
        (IndexDefinitionMultiValue) indexDefinitions.get(multiValueDefinitionIndex);

    final var compositeWrapperKeysToAdd =
        new CompositeWrapperMap(
            session, keysToAdd, indexDefinitions, params, multiValueDefinitionIndex);

    final var compositeWrapperKeysToRemove =
        new CompositeWrapperMap(
            session, keysToRemove, indexDefinitions, params, multiValueDefinitionIndex);

    indexDefinitionMultiValue.processChangeEvent(
        session, changeEvent, compositeWrapperKeysToAdd, compositeWrapperKeysToRemove);
  }

  /**
   * {@inheritDoc}
   */
  public int getParamCount() {
    var total = 0;
    for (final var indexDefinition : indexDefinitions) {
      total += indexDefinition.getParamCount();
    }
    return total;
  }

  /**
   * {@inheritDoc}
   */
  public PropertyType[] getTypes() {
    final List<PropertyType> types = new LinkedList<>();
    for (final var indexDefinition : indexDefinitions) {
      Collections.addAll(types, indexDefinition.getTypes());
    }

    return types.toArray(new PropertyType[0]);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final var that = (CompositeIndexDefinition) o;

    if (!className.equals(that.className)) {
      return false;
    }
    return indexDefinitions.equals(that.indexDefinitions);
  }

  @Override
  public int hashCode() {
    var result = indexDefinitions.hashCode();
    result = 31 * result + className.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "CompositeIndexDefinition{"
        + "indexDefinitions="
        + indexDefinitions
        + ", className='"
        + className
        + '\''
        + '}';
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public @Nonnull EntityImpl toStream(DatabaseSessionInternal db, @Nonnull EntityImpl entity) {
    serializeToStream(db, entity);
    return entity;
  }

  @Override
  protected void serializeToStream(DatabaseSessionInternal db, EntityImpl entity) {
    super.serializeToStream(db, entity);

    final List<EntityImpl> inds = new ArrayList<>(indexDefinitions.size());
    final List<String> indClasses = new ArrayList<>(indexDefinitions.size());

    entity.setPropertyInternal("className", className);
    for (final var indexDefinition : indexDefinitions) {
      final var indexEntity = indexDefinition.toStream(db, new EntityImpl(db));
      inds.add(indexEntity);

      indClasses.add(indexDefinition.getClass().getName());
    }
    entity.setPropertyInternal("indexDefinitions", inds, PropertyType.EMBEDDEDLIST);
    entity.setPropertyInternal("indClasses", indClasses, PropertyType.EMBEDDEDLIST);
    entity.setPropertyInternal("nullValuesIgnored", isNullValuesIgnored());
  }

  /**
   * {@inheritDoc}
   */
  public String toCreateIndexDDL(final String indexName, final String indexType, String engine) {
    final var ddl = new StringBuilder("create index ");
    ddl.append('`').append(indexName).append('`').append(" on ").append(className).append(" ( ");

    final var fieldIterator = getFieldsToIndex().iterator();
    if (fieldIterator.hasNext()) {
      ddl.append(quoteFieldName(fieldIterator.next()));
      while (fieldIterator.hasNext()) {
        ddl.append(", ").append(quoteFieldName(fieldIterator.next()));
      }
    }
    ddl.append(" ) ").append(indexType).append(' ');

    if (engine != null) {
      ddl.append(CommandExecutorSQLCreateIndex.KEYWORD_ENGINE + " ").append(engine).append(' ');
    }

    if (multiValueDefinitionIndex == -1) {
      var first = true;
      for (var oType : getTypes()) {
        if (first) {
          first = false;
        } else {
          ddl.append(", ");
        }

        ddl.append(oType.name());
      }
    }

    return ddl.toString();
  }

  private String quoteFieldName(String next) {
    if (next == null) {
      return null;
    }
    next = next.trim();
    if (next.startsWith("`")) {
      return next;
    }
    if (next.toLowerCase(Locale.ENGLISH).endsWith("collate ci")) {
      next = next.substring(0, next.length() - "collate ci".length());
      return "`" + next.trim() + "` collate ci";
    }
    return "`" + next + "`";
  }

  public void fromStream(@Nonnull EntityImpl entity) {
    serializeFromStream(entity);
  }

  @Override
  protected void serializeFromStream(EntityImpl entity) {
    super.serializeFromStream(entity);

    try {
      className = entity.field("className");

      final List<EntityImpl> inds = entity.field("indexDefinitions");
      final List<String> indClasses = entity.field("indClasses");

      indexDefinitions.clear();

      collate = new CompositeCollate(this);

      for (var i = 0; i < indClasses.size(); i++) {
        final var clazz = Class.forName(indClasses.get(i));
        final var indEntity = inds.get(i);

        final var indexDefinition =
            (IndexDefinition) clazz.getDeclaredConstructor().newInstance();
        indexDefinition.fromStream(indEntity);

        indexDefinitions.add(indexDefinition);
        collate.addCollate(indexDefinition.getCollate());

        if (indexDefinition instanceof IndexDefinitionMultiValue) {
          multiValueDefinitionIndex = indexDefinitions.size() - 1;
        }
      }

      setNullValuesIgnored(!Boolean.FALSE.equals(entity.<Boolean>field("nullValuesIgnored")));
    } catch (final ClassNotFoundException
                   | InvocationTargetException
                   | InstantiationException
                   | IllegalAccessException
                   | NoSuchMethodException e) {
      throw BaseException.wrapException(
          new IndexException("Error during composite index deserialization"), e);
    }
  }

  @Override
  public Collate getCollate() {
    return collate;
  }

  @Override
  public void setCollate(Collate collate) {
    throw new UnsupportedOperationException();
  }

  private static final class CompositeWrapperMap implements Object2IntMap<Object> {

    private final Object2IntOpenHashMap<CompositeKey> underlying;
    private final Object[] params;
    private final List<IndexDefinition> indexDefinitions;
    private final int multiValueIndex;
    private final DatabaseSessionInternal session;

    private CompositeWrapperMap(
        DatabaseSessionInternal session,
        Object2IntOpenHashMap<CompositeKey> underlying,
        List<IndexDefinition> indexDefinitions,
        Object[] params,
        int multiValueIndex) {
      this.session = session;
      this.underlying = underlying;
      this.params = params;
      this.multiValueIndex = multiValueIndex;
      this.indexDefinitions = indexDefinitions;
    }

    public int size() {
      return underlying.size();
    }

    public boolean isEmpty() {
      return underlying.isEmpty();
    }

    public boolean containsKey(Object key) {
      final var compositeKey = convertToCompositeKey(session, key);

      return underlying.containsKey(compositeKey);
    }

    @Override
    public void defaultReturnValue(int i) {
      underlying.defaultReturnValue(i);
    }

    @Override
    public int defaultReturnValue() {
      return underlying.defaultReturnValue();
    }

    @Override
    public ObjectSet<Entry<Object>> object2IntEntrySet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(int i) {
      return underlying.containsValue(i);
    }

    @Override
    public int getInt(Object o) {
      return underlying.getInt(convertToCompositeKey(session, o));
    }

    public int put(Object key, int value) {
      final var compositeKey = convertToCompositeKey(session, key);
      return underlying.put(compositeKey, value);
    }

    public int removeInt(Object key) {
      return underlying.removeInt(convertToCompositeKey(session, key));
    }

    public void clear() {
      underlying.clear();
    }

    @Nonnull
    @Override
    public ObjectSet<Object> keySet() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void putAll(@Nonnull Map<?, ? extends Integer> m) {
      throw new UnsupportedOperationException();
    }

    @Nonnull
    public IntCollection values() {
      return underlying.values();
    }

    private CompositeKey convertToCompositeKey(DatabaseSessionInternal session, Object key) {
      final var compositeKey = new CompositeKey();

      var paramsIndex = 0;
      for (var i = 0; i < indexDefinitions.size(); i++) {
        final var indexDefinition = indexDefinitions.get(i);
        if (i != multiValueIndex) {
          compositeKey.addKey(indexDefinition.createValue(session, params[paramsIndex]));
          paramsIndex++;
        } else {
          compositeKey.addKey(
              ((IndexDefinitionMultiValue) indexDefinition).createSingleValue(session, key));
        }
      }
      return compositeKey;
    }
  }

  @Override
  public boolean isAutomatic() {
    return indexDefinitions.get(0).isAutomatic();
  }
}
