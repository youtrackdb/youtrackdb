/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.orientechnologies.spatial;

import static com.orientechnologies.lucene.OLuceneIndexFactory.LUCENE_ALGORITHM;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.core.YouTrackDBManager;
import com.orientechnologies.core.config.IndexEngineData;
import com.orientechnologies.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTConfigurationException;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.index.OIndexFactory;
import com.orientechnologies.core.index.OIndexInternal;
import com.orientechnologies.core.index.OIndexMetadata;
import com.orientechnologies.core.index.engine.OBaseIndexEngine;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.storage.OStorage;
import com.orientechnologies.spatial.engine.OLuceneSpatialIndexEngineDelegator;
import com.orientechnologies.spatial.index.OLuceneSpatialIndex;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class OLuceneSpatialIndexFactory implements OIndexFactory, ODatabaseLifecycleListener {

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(YTClass.INDEX_TYPE.SPATIAL.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(LUCENE_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  private final OLuceneSpatialManager spatialManager;

  public OLuceneSpatialIndexFactory() {
    this(false);
  }

  public OLuceneSpatialIndexFactory(boolean manual) {
    if (!manual) {
      YouTrackDBManager.instance().addDbLifecycleListener(this);
    }

    spatialManager = new OLuceneSpatialManager(OShapeFactory.INSTANCE);
  }

  @Override
  public int getLastVersion(final String algorithm) {
    return 0;
  }

  @Override
  public Set<String> getTypes() {
    return TYPES;
  }

  @Override
  public Set<String> getAlgorithms() {
    return ALGORITHMS;
  }

  @Override
  public OIndexInternal createIndex(OStorage storage, OIndexMetadata im)
      throws YTConfigurationException {
    var metadata = im.getMetadata();
    final String indexType = im.getType();
    final String algorithm = im.getAlgorithm();

    OBinarySerializer<?> objectSerializer =
        storage
            .getComponentsFactory()
            .binarySerializerFactory
            .getObjectSerializer(OLuceneMockSpatialSerializer.INSTANCE.getId());

    if (objectSerializer == null) {
      storage
          .getComponentsFactory()
          .binarySerializerFactory
          .registerSerializer(OLuceneMockSpatialSerializer.INSTANCE, YTType.EMBEDDED);
    }

    if (metadata == null) {
      var metadataDoc = new YTEntityImpl();
      metadataDoc.field("analyzer", StandardAnalyzer.class.getName());
      im.setMetadata(metadataDoc);
    }

    if (YTClass.INDEX_TYPE.SPATIAL.toString().equals(indexType)) {
      return new OLuceneSpatialIndex(im, storage);
    }
    throw new YTConfigurationException("Unsupported type : " + algorithm);
  }

  @Override
  public OBaseIndexEngine createIndexEngine(OStorage storage, IndexEngineData data) {
    return new OLuceneSpatialIndexEngineDelegator(
        data.getIndexId(), data.getName(), storage, data.getVersion());
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(YTDatabaseSessionInternal iDatabase) {
    spatialManager.init(iDatabase);
  }

  @Override
  public void onOpen(YTDatabaseSessionInternal iDatabase) {
  }

  @Override
  public void onClose(YTDatabaseSessionInternal iDatabase) {
  }

  @Override
  public void onDrop(final YTDatabaseSessionInternal db) {
    try {
      if (db.isClosed()) {
        return;
      }

      OLogManager.instance().debug(this, "Dropping spatial indexes...");
      final YTDatabaseSessionInternal internalDb = db;
      for (OIndex idx : internalDb.getMetadata().getIndexManagerInternal().getIndexes(internalDb)) {

        if (idx.getInternal() instanceof OLuceneSpatialIndex) {
          OLogManager.instance().debug(this, "- index '%s'", idx.getName());
          internalDb.getMetadata().getIndexManager().dropIndex(idx.getName());
        }
      }
    } catch (Exception e) {
      OLogManager.instance().warn(this, "Error on dropping spatial indexes", e);
    }
  }

  @Override
  public void onCreateClass(YTDatabaseSessionInternal iDatabase, YTClass iClass) {
  }

  @Override
  public void onDropClass(YTDatabaseSessionInternal iDatabase, YTClass iClass) {
  }

  @Override
  public void onLocalNodeConfigurationRequest(YTEntityImpl iConfiguration) {
  }
}
