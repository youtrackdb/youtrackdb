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

import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream.MixedIndexRIDContainerSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream.StreamSerializerRID;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream.StreamSerializerSBTreeIndexRIDContainer;
import com.jetbrains.youtrack.db.internal.core.sharding.auto.AutoShardingIndexFactory;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.HashIndexFactory;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;

/**
 * Contains the index metadata.
 */
public class IndexMetadata {

  @Nonnull
  private final String name;
  private final IndexDefinition indexDefinition;
  private final Set<String> clustersToIndex;
  private final String type;
  private final String algorithm;
  private final String valueContainerAlgorithm;
  private int version;
  private Map<String, ?> metadata;

  public IndexMetadata(
      @Nonnull String name,
      IndexDefinition indexDefinition,
      Set<String> clustersToIndex,
      String type,
      String algorithm,
      String valueContainerAlgorithm,
      int version,
      Map<String, ?> metadata) {
    this.name = name;
    this.indexDefinition = indexDefinition;
    this.clustersToIndex = clustersToIndex;
    this.type = type;
    if (type.equalsIgnoreCase(SchemaClass.INDEX_TYPE.UNIQUE_HASH_INDEX.name())
        || type.equalsIgnoreCase(SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.name())
        || type.equalsIgnoreCase(SchemaClass.INDEX_TYPE.DICTIONARY_HASH_INDEX.name())) {
      if (!algorithm.equalsIgnoreCase("autosharding")) {
        algorithm = HashIndexFactory.HASH_INDEX_ALGORITHM;
      }
    }
    this.algorithm = algorithm;
    this.valueContainerAlgorithm = Objects.requireNonNullElse(valueContainerAlgorithm,
        AutoShardingIndexFactory.NONE_VALUE_CONTAINER);
    this.version = version;
    this.metadata = metadata;
  }


  @Nonnull
  public String getName() {
    return name;
  }

  public IndexDefinition getIndexDefinition() {
    return indexDefinition;
  }

  public Set<String> getClustersToIndex() {
    return clustersToIndex;
  }

  public String getType() {
    return type;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final IndexMetadata that = (IndexMetadata) o;

    if (!Objects.equals(algorithm, that.algorithm)) {
      return false;
    }
    if (!clustersToIndex.equals(that.clustersToIndex)) {
      return false;
    }
    if (!Objects.equals(indexDefinition, that.indexDefinition)) {
      return false;
    }
    if (!name.equals(that.name)) {
      return false;
    }
    return type.equals(that.type);
  }

  @Override
  public int hashCode() {
    int result = name.hashCode();
    result = 31 * result + (indexDefinition != null ? indexDefinition.hashCode() : 0);
    result = 31 * result + clustersToIndex.hashCode();
    result = 31 * result + type.hashCode();
    result = 31 * result + (algorithm != null ? algorithm.hashCode() : 0);
    return result;
  }

  public String getValueContainerAlgorithm() {
    return valueContainerAlgorithm;
  }

  public boolean isMultivalue() {
    String t = type.toUpperCase();
    return SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(t)
        || SchemaClass.INDEX_TYPE.NOTUNIQUE.toString().equals(t)
        || SchemaClass.INDEX_TYPE.FULLTEXT.toString().equals(t);
  }

  public byte getValueSerializerId(int binaryFormatVersion) {
    String t = type.toUpperCase();
    if (SchemaClass.INDEX_TYPE.NOTUNIQUE_HASH_INDEX.toString().equals(t)
        || SchemaClass.INDEX_TYPE.NOTUNIQUE.toString().equals(t)
        || SchemaClass.INDEX_TYPE.FULLTEXT.toString().equals(t)
        || SchemaClass.INDEX_TYPE.SPATIAL.toString().equals(t)) {
      // TODO: Hard Coded Lucene maybe fix
      if (binaryFormatVersion >= 13 && !"LUCENE".equalsIgnoreCase(algorithm)) {
        return MixedIndexRIDContainerSerializer.ID;
      }

      return StreamSerializerSBTreeIndexRIDContainer.ID;
    } else {
      return StreamSerializerRID.INSTANCE.getId();
    }
  }

  public int getVersion() {
    return version;
  }

  public void setVersion(int version) {
    this.version = version;
  }

  public Map<String, ?> getMetadata() {
    return metadata;
  }

  public void setMetadata(Map<String, ?> metadata) {
    this.metadata = metadata;
  }
}
