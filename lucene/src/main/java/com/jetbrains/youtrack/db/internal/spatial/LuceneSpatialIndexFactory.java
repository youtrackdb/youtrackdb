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
package com.jetbrains.youtrack.db.internal.spatial;

import static com.jetbrains.youtrack.db.internal.lucene.LuceneIndexFactory.LUCENE_ALGORITHM;

import com.jetbrains.youtrack.db.api.exception.ConfigurationException;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.config.IndexEngineData;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseLifecycleListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.IndexFactory;
import com.jetbrains.youtrack.db.internal.core.index.IndexInternal;
import com.jetbrains.youtrack.db.internal.core.index.IndexMetadata;
import com.jetbrains.youtrack.db.internal.core.index.engine.BaseIndexEngine;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.spatial.engine.LuceneSpatialIndexEngineDelegator;
import com.jetbrains.youtrack.db.internal.spatial.index.LuceneSpatialIndex;
import com.jetbrains.youtrack.db.internal.spatial.shape.ShapeFactory;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

public class LuceneSpatialIndexFactory implements IndexFactory, DatabaseLifecycleListener {

  private static final Set<String> TYPES;
  private static final Set<String> ALGORITHMS;

  static {
    final Set<String> types = new HashSet<String>();
    types.add(SchemaClass.INDEX_TYPE.SPATIAL.toString());
    TYPES = Collections.unmodifiableSet(types);
  }

  static {
    final Set<String> algorithms = new HashSet<String>();
    algorithms.add(LUCENE_ALGORITHM);
    ALGORITHMS = Collections.unmodifiableSet(algorithms);
  }

  private final LuceneSpatialManager spatialManager;

  public LuceneSpatialIndexFactory() {
    this(false);
  }

  public LuceneSpatialIndexFactory(boolean manual) {
    if (!manual) {
      YouTrackDBEnginesManager.instance().addDbLifecycleListener(this);
    }

    spatialManager = new LuceneSpatialManager(ShapeFactory.INSTANCE);
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
  public IndexInternal createIndex(Storage storage, IndexMetadata im)
      throws ConfigurationException {
    var metadata = im.getMetadata();
    final var indexType = im.getType();
    final var algorithm = im.getAlgorithm();

    if (metadata == null || !metadata.containsKey("analyzer")) {
      HashMap<String, Object> met;
      if (metadata != null) {
        met = new HashMap<>(metadata);
      } else {
        met = new HashMap<>();
      }

      met.put("analyzer", StandardAnalyzer.class.getName());
      im.setMetadata(met);
    }

    if (SchemaClass.INDEX_TYPE.SPATIAL.toString().equals(indexType)) {
      return new LuceneSpatialIndex(im, storage);
    }

    throw new ConfigurationException(storage.getName(), "Unsupported type : " + algorithm);
  }

  @Override
  public BaseIndexEngine createIndexEngine(Storage storage, IndexEngineData data) {
    return new LuceneSpatialIndexEngineDelegator(
        data.getIndexId(), data.getName(), storage, data.getVersion());
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.REGULAR;
  }

  @Override
  public void onCreate(@Nonnull DatabaseSessionInternal session) {
    spatialManager.init(session);
  }

  @Override
  public void onOpen(@Nonnull DatabaseSessionInternal session) {
  }

  @Override
  public void onClose(@Nonnull DatabaseSessionInternal session) {
  }

  @Override
  public void onDrop(final @Nonnull DatabaseSessionInternal session) {
    try {
      if (session.isClosed()) {
        return;
      }

      LogManager.instance().debug(this, "Dropping spatial indexes...");
      final var internalDb = session;
      for (var idx : internalDb.getMetadata().getIndexManagerInternal().getIndexes(internalDb)) {

        if (idx.getInternal() instanceof LuceneSpatialIndex) {
          LogManager.instance().debug(this, "- index '%s'", idx.getName());
          internalDb.getMetadata().getIndexManager().dropIndex(idx.getName());
        }
      }
    } catch (Exception e) {
      LogManager.instance().warn(this, "Error on dropping spatial indexes", e);
    }
  }

  @Override
  public void onDropClass(DatabaseSessionInternal session, SchemaClass iClass) {
  }

}
