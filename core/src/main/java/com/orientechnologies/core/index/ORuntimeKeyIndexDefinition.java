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
package com.orientechnologies.core.index;

import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTConfigurationException;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.serialization.serializer.binary.OBinarySerializerFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Index definition that use the serializer specified at run-time not based on type. This is useful
 * to have custom type keys for indexes.
 */
public class ORuntimeKeyIndexDefinition<T> extends OAbstractIndexDefinition {

  private transient OBinarySerializer<T> serializer;

  @SuppressWarnings("unchecked")
  public ORuntimeKeyIndexDefinition(final byte iId) {
    super();

    serializer =
        (OBinarySerializer<T>) OBinarySerializerFactory.getInstance().getObjectSerializer(iId);
    if (serializer == null) {
      throw new YTConfigurationException(
          "Runtime index definition cannot find binary serializer with id="
              + iId
              + ". Assure to plug custom serializer into the server.");
    }
  }

  public ORuntimeKeyIndexDefinition() {
  }

  public List<String> getFields() {
    return Collections.emptyList();
  }

  public List<String> getFieldsToIndex() {
    return Collections.emptyList();
  }

  public String getClassName() {
    return null;
  }

  public Comparable<?> createValue(YTDatabaseSessionInternal session, final List<?> params) {
    return (Comparable<?>) refreshRid(session, params.get(0));
  }

  public Comparable<?> createValue(YTDatabaseSessionInternal session, final Object... params) {
    return createValue(session, Arrays.asList(params));
  }

  public int getParamCount() {
    return 1;
  }

  public YTType[] getTypes() {
    return new YTType[0];
  }

  @Override
  public @Nonnull YTEntityImpl toStream(@Nonnull YTEntityImpl document) {
    serializeToStream(document);
    return document;
  }

  @Override
  protected void serializeToStream(YTEntityImpl document) {
    super.serializeToStream(document);

    document.setProperty("keySerializerId", serializer.getId());
    document.setProperty("collate", collate.getName());
    document.setProperty("nullValuesIgnored", isNullValuesIgnored());
  }

  public void fromStream(@Nonnull YTEntityImpl document) {
    serializeFromStream(document);
  }

  @Override
  protected void serializeFromStream(YTEntityImpl document) {
    super.serializeFromStream(document);

    final byte keySerializerId = ((Number) document.field("keySerializerId")).byteValue();
    //noinspection unchecked
    serializer =
        (OBinarySerializer<T>)
            OBinarySerializerFactory.getInstance().getObjectSerializer(keySerializerId);
    if (serializer == null) {
      throw new YTConfigurationException(
          "Runtime index definition cannot find binary serializer with id="
              + keySerializerId
              + ". Assure to plug custom serializer into the server.");
    }

    setNullValuesIgnored(!Boolean.FALSE.equals(document.<Boolean>field("nullValuesIgnored")));
  }

  public Object getDocumentValueToIndex(
      YTDatabaseSessionInternal session, final YTEntityImpl iDocument) {
    throw new YTIndexException("This method is not supported in given index definition.");
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final ORuntimeKeyIndexDefinition<?> that = (ORuntimeKeyIndexDefinition<?>) o;
    return serializer.equals(that.serializer);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + serializer.getId();
    return result;
  }

  @Override
  public String toString() {
    return "ORuntimeKeyIndexDefinition{" + "serializer=" + serializer.getId() + '}';
  }

  /**
   * {@inheritDoc}
   */
  public String toCreateIndexDDL(final String indexName, final String indexType, String engine) {
    return "create index `" + indexName + "` " + indexType + ' ' + "runtime " + serializer.getId();
  }

  public OBinarySerializer<T> getSerializer() {
    return serializer;
  }

  @Override
  public boolean isAutomatic() {
    return getClassName() != null;
  }
}
