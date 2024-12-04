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
package com.orientechnologies.orient.core.index;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.collate.OCollate;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.OMultiValueChangeEvent;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.OCommandExecutorSQLCreateIndex;
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
 * Index that consist of several indexDefinitions like {@link OPropertyIndexDefinition}.
 */
public class OCompositeIndexDefinition extends OAbstractIndexDefinition {

  private final List<OIndexDefinition> indexDefinitions;
  private String className;
  private int multiValueDefinitionIndex = -1;
  private OCompositeCollate collate = new OCompositeCollate(this);

  public OCompositeIndexDefinition() {
    indexDefinitions = new ArrayList<>(5);
  }

  /**
   * Constructor for new index creation.
   *
   * @param iClassName - name of class which is owner of this index
   */
  public OCompositeIndexDefinition(final String iClassName) {
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
  public OCompositeIndexDefinition(
      final String iClassName, final List<? extends OIndexDefinition> iIndexes) {
    super();

    indexDefinitions = new ArrayList<>(5);
    for (OIndexDefinition indexDefinition : iIndexes) {
      indexDefinitions.add(indexDefinition);
      collate.addCollate(indexDefinition.getCollate());

      if (indexDefinition instanceof OIndexDefinitionMultiValue) {
        if (multiValueDefinitionIndex == -1) {
          multiValueDefinitionIndex = indexDefinitions.size() - 1;
        } else {
          throw new YTIndexException("Composite key cannot contain more than one collection item");
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
  public void addIndex(final OIndexDefinition indexDefinition) {
    indexDefinitions.add(indexDefinition);
    if (indexDefinition instanceof OIndexDefinitionMultiValue) {
      if (multiValueDefinitionIndex == -1) {
        multiValueDefinitionIndex = indexDefinitions.size() - 1;
      } else {
        throw new YTIndexException("Composite key cannot contain more than one collection item");
      }
    }

    collate.addCollate(indexDefinition.getCollate());
  }

  /**
   * {@inheritDoc}
   */
  public List<String> getFields() {
    final List<String> fields = new LinkedList<>();
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      fields.addAll(indexDefinition.getFields());
    }
    return Collections.unmodifiableList(fields);
  }

  /**
   * {@inheritDoc}
   */
  public List<String> getFieldsToIndex() {
    final List<String> fields = new LinkedList<>();
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      fields.addAll(indexDefinition.getFieldsToIndex());
    }
    return Collections.unmodifiableList(fields);
  }

  /**
   * {@inheritDoc}
   */
  public Object getDocumentValueToIndex(
      YTDatabaseSessionInternal session, final YTDocument iDocument) {
    final List<OCompositeKey> compositeKeys = new ArrayList<>(10);
    final OCompositeKey firstKey = new OCompositeKey();
    boolean containsCollection = false;

    compositeKeys.add(firstKey);

    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      final Object result = indexDefinition.getDocumentValueToIndex(session, iDocument);

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
  public Object createValue(YTDatabaseSessionInternal session, final List<?> params) {
    int currentParamIndex = 0;
    final OCompositeKey firstKey = new OCompositeKey();

    final List<OCompositeKey> compositeKeys = new ArrayList<>(10);
    compositeKeys.add(firstKey);

    boolean containsCollection = false;

    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      if (currentParamIndex + 1 > params.size()) {
        break;
      }

      final int endIndex =
          Math.min(currentParamIndex + indexDefinition.getParamCount(), params.size());
      final List<?> indexParams = params.subList(currentParamIndex, endIndex);
      currentParamIndex += indexDefinition.getParamCount();

      final Object keyValue = indexDefinition.createValue(session, indexParams);

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

  public OIndexDefinitionMultiValue getMultiValueDefinition() {
    if (multiValueDefinitionIndex > -1) {
      return (OIndexDefinitionMultiValue) indexDefinitions.get(multiValueDefinitionIndex);
    }

    return null;
  }

  public OCompositeKey createSingleValue(YTDatabaseSessionInternal session, final List<?> params) {
    final OCompositeKey compositeKey = new OCompositeKey();
    int currentParamIndex = 0;

    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      if (currentParamIndex + 1 > params.size()) {
        break;
      }

      final int endIndex =
          Math.min(currentParamIndex + indexDefinition.getParamCount(), params.size());

      final List<?> indexParams = params.subList(currentParamIndex, endIndex);
      currentParamIndex += indexDefinition.getParamCount();

      final Object keyValue;

      if (indexDefinition instanceof OIndexDefinitionMultiValue) {
        keyValue =
            ((OIndexDefinitionMultiValue) indexDefinition)
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
      OCompositeKey firstKey,
      List<OCompositeKey> compositeKeys,
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
        for (int i = 1; i < collectionSize; i++) {
          final OCompositeKey compositeKey = new OCompositeKey(firstKey.getKeys());
          compositeKeys.add(compositeKey);
        }
      } else {
        throw new YTIndexException("Composite key cannot contain more than one collection item");
      }

      int compositeIndex = 0;
      if (!collectionKey.isEmpty()) {
        for (final Object keyItem : collectionKey) {
          final OCompositeKey compositeKey = compositeKeys.get(compositeIndex);
          compositeKey.addKey(keyItem);

          compositeIndex++;
        }
      } else {
        firstKey.addKey(null);
      }

      containsCollection = true;
    } else if (containsCollection) {
      for (final OCompositeKey compositeKey : compositeKeys) {
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
  public Object createValue(YTDatabaseSessionInternal session, final Object... params) {
    if (params.length == 1 && params[0] instanceof Collection) {
      return params[0];
    }

    return createValue(session, Arrays.asList(params));
  }

  public void processChangeEvent(
      YTDatabaseSessionInternal session,
      OMultiValueChangeEvent<?, ?> changeEvent,
      Object2IntOpenHashMap<OCompositeKey> keysToAdd,
      Object2IntOpenHashMap<OCompositeKey> keysToRemove,
      Object... params) {

    final OIndexDefinitionMultiValue indexDefinitionMultiValue =
        (OIndexDefinitionMultiValue) indexDefinitions.get(multiValueDefinitionIndex);

    final CompositeWrapperMap compositeWrapperKeysToAdd =
        new CompositeWrapperMap(
            session, keysToAdd, indexDefinitions, params, multiValueDefinitionIndex);

    final CompositeWrapperMap compositeWrapperKeysToRemove =
        new CompositeWrapperMap(
            session, keysToRemove, indexDefinitions, params, multiValueDefinitionIndex);

    indexDefinitionMultiValue.processChangeEvent(
        session, changeEvent, compositeWrapperKeysToAdd, compositeWrapperKeysToRemove);
  }

  /**
   * {@inheritDoc}
   */
  public int getParamCount() {
    int total = 0;
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      total += indexDefinition.getParamCount();
    }
    return total;
  }

  /**
   * {@inheritDoc}
   */
  public YTType[] getTypes() {
    final List<YTType> types = new LinkedList<>();
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      Collections.addAll(types, indexDefinition.getTypes());
    }

    return types.toArray(new YTType[0]);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OCompositeIndexDefinition that = (OCompositeIndexDefinition) o;

    if (!className.equals(that.className)) {
      return false;
    }
    return indexDefinitions.equals(that.indexDefinitions);
  }

  @Override
  public int hashCode() {
    int result = indexDefinitions.hashCode();
    result = 31 * result + className.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "OCompositeIndexDefinition{"
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
  public @Nonnull YTDocument toStream(@Nonnull YTDocument document) {
    serializeToStream(document);
    return document;
  }

  @Override
  protected void serializeToStream(YTDocument document) {
    super.serializeToStream(document);

    final List<YTDocument> inds = new ArrayList<>(indexDefinitions.size());
    final List<String> indClasses = new ArrayList<>(indexDefinitions.size());

    document.setPropertyInternal("className", className);
    for (final OIndexDefinition indexDefinition : indexDefinitions) {
      final YTDocument indexDocument = indexDefinition.toStream(new YTDocument());
      inds.add(indexDocument);

      indClasses.add(indexDefinition.getClass().getName());
    }
    document.setPropertyInternal("indexDefinitions", inds, YTType.EMBEDDEDLIST);
    document.setPropertyInternal("indClasses", indClasses, YTType.EMBEDDEDLIST);
    document.setPropertyInternal("nullValuesIgnored", isNullValuesIgnored());
  }

  /**
   * {@inheritDoc}
   */
  public String toCreateIndexDDL(final String indexName, final String indexType, String engine) {
    final StringBuilder ddl = new StringBuilder("create index ");
    ddl.append('`').append(indexName).append('`').append(" on ").append(className).append(" ( ");

    final Iterator<String> fieldIterator = getFieldsToIndex().iterator();
    if (fieldIterator.hasNext()) {
      ddl.append(quoteFieldName(fieldIterator.next()));
      while (fieldIterator.hasNext()) {
        ddl.append(", ").append(quoteFieldName(fieldIterator.next()));
      }
    }
    ddl.append(" ) ").append(indexType).append(' ');

    if (engine != null) {
      ddl.append(OCommandExecutorSQLCreateIndex.KEYWORD_ENGINE + " ").append(engine).append(' ');
    }

    if (multiValueDefinitionIndex == -1) {
      boolean first = true;
      for (YTType oType : getTypes()) {
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

  public void fromStream(@Nonnull YTDocument document) {
    serializeFromStream(document);
  }

  @Override
  protected void serializeFromStream(YTDocument document) {
    super.serializeFromStream(document);

    try {
      className = document.field("className");

      final List<YTDocument> inds = document.field("indexDefinitions");
      final List<String> indClasses = document.field("indClasses");

      indexDefinitions.clear();

      collate = new OCompositeCollate(this);

      for (int i = 0; i < indClasses.size(); i++) {
        final Class<?> clazz = Class.forName(indClasses.get(i));
        final YTDocument indDoc = inds.get(i);

        final OIndexDefinition indexDefinition =
            (OIndexDefinition) clazz.getDeclaredConstructor().newInstance();
        indexDefinition.fromStream(indDoc);

        indexDefinitions.add(indexDefinition);
        collate.addCollate(indexDefinition.getCollate());

        if (indexDefinition instanceof OIndexDefinitionMultiValue) {
          multiValueDefinitionIndex = indexDefinitions.size() - 1;
        }
      }

      setNullValuesIgnored(!Boolean.FALSE.equals(document.<Boolean>field("nullValuesIgnored")));
    } catch (final ClassNotFoundException
                   | InvocationTargetException
                   | InstantiationException
                   | IllegalAccessException
                   | NoSuchMethodException e) {
      throw YTException.wrapException(
          new YTIndexException("Error during composite index deserialization"), e);
    }
  }

  @Override
  public OCollate getCollate() {
    return collate;
  }

  @Override
  public void setCollate(OCollate collate) {
    throw new UnsupportedOperationException();
  }

  private static final class CompositeWrapperMap implements Object2IntMap<Object> {

    private final Object2IntOpenHashMap<OCompositeKey> underlying;
    private final Object[] params;
    private final List<OIndexDefinition> indexDefinitions;
    private final int multiValueIndex;
    private final YTDatabaseSessionInternal session;

    private CompositeWrapperMap(
        YTDatabaseSessionInternal session,
        Object2IntOpenHashMap<OCompositeKey> underlying,
        List<OIndexDefinition> indexDefinitions,
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
      final OCompositeKey compositeKey = convertToCompositeKey(session, key);

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
      final OCompositeKey compositeKey = convertToCompositeKey(session, key);
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

    private OCompositeKey convertToCompositeKey(YTDatabaseSessionInternal session, Object key) {
      final OCompositeKey compositeKey = new OCompositeKey();

      int paramsIndex = 0;
      for (int i = 0; i < indexDefinitions.size(); i++) {
        final OIndexDefinition indexDefinition = indexDefinitions.get(i);
        if (i != multiValueIndex) {
          compositeKey.addKey(indexDefinition.createValue(session, params[paramsIndex]));
          paramsIndex++;
        } else {
          compositeKey.addKey(
              ((OIndexDefinitionMultiValue) indexDefinition).createSingleValue(session, key));
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
