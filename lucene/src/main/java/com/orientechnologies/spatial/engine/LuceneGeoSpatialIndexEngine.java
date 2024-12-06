/*
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 * <p>
 * *
 */
package com.orientechnologies.spatial.engine;

import static com.orientechnologies.lucene.builder.OLuceneQueryBuilder.EMPTY_METADATA;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.ContextualRecordId;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.IndexEngineException;
import com.jetbrains.youtrack.db.internal.core.index.IndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.AtomicOperation;
import com.orientechnologies.lucene.collections.OLuceneResultSet;
import com.orientechnologies.lucene.collections.OLuceneResultSetEmpty;
import com.orientechnologies.lucene.query.OLuceneQueryContext;
import com.orientechnologies.lucene.tx.OLuceneTxChanges;
import com.orientechnologies.spatial.factory.OSpatialStrategyFactory;
import com.orientechnologies.spatial.query.OSpatialQueryContext;
import com.orientechnologies.spatial.shape.OShapeBuilder;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.spatial.SpatialStrategy;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Point;

public class LuceneGeoSpatialIndexEngine extends LuceneSpatialIndexEngineAbstract {

  public LuceneGeoSpatialIndexEngine(
      Storage storage, String name, int id, OShapeBuilder factory) {
    super(storage, name, id, factory);
  }

  @Override
  protected SpatialStrategy createSpatialStrategy(
      IndexDefinition indexDefinition, Map<String, ?> metadata) {

    return OSpatialStrategyFactory.createStrategy(ctx, getDatabase(), indexDefinition);
  }

  @Override
  public Object get(DatabaseSessionInternal session, Object key) {
    return getInTx(session, key, null);
  }

  @Override
  public Set<Identifiable> getInTx(DatabaseSessionInternal session, Object key,
      OLuceneTxChanges changes) {
    updateLastAccess();
    openIfClosed();
    try {
      if (key instanceof Map) {
        //noinspection unchecked
        return newGeoSearch((Map<String, Object>) key, changes);
      }
    } catch (Exception e) {
      if (e instanceof BaseException forward) {
        throw forward;
      } else {
        throw BaseException.wrapException(new IndexEngineException("Error parsing lucene query"),
            e);
      }
    }

    return new OLuceneResultSetEmpty();
  }

  private Set<Identifiable> newGeoSearch(Map<String, Object> key, OLuceneTxChanges changes)
      throws Exception {

    OLuceneQueryContext queryContext = queryStrategy.build(key).withChanges(changes);
    return new OLuceneResultSet(this, queryContext, EMPTY_METADATA);
  }

  @Override
  public void onRecordAddedToResultSet(
      OLuceneQueryContext queryContext,
      ContextualRecordId recordId,
      Document doc,
      ScoreDoc score) {

    OSpatialQueryContext spatialContext = (OSpatialQueryContext) queryContext;
    if (spatialContext.spatialArgs != null) {
      updateLastAccess();
      openIfClosed();
      @SuppressWarnings("deprecation")
      Point docPoint = (Point) ctx.readShape(doc.get(strategy.getFieldName()));
      double docDistDEG =
          ctx.getDistCalc().distance(spatialContext.spatialArgs.getShape().getCenter(), docPoint);
      final double docDistInKM =
          DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);
      Map<String, Object> data = new HashMap<String, Object>();
      data.put("distance", docDistInKM);
      recordId.setContext(data);
    }
  }

  @Override
  public void put(DatabaseSessionInternal session, AtomicOperation atomicOperation, Object key,
      Object value) {

    if (key instanceof Identifiable) {
      openIfClosed();
      EntityImpl location = ((Identifiable) key).getRecord();
      updateLastAccess();
      addDocument(newGeoDocument((Identifiable) value, factory.fromDoc(location), location));
    }
  }

  @Override
  public void update(
      DatabaseSessionInternal session, AtomicOperation atomicOperation, Object key,
      IndexKeyUpdater<Object> updater) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean validatedPut(
      AtomicOperation atomicOperation,
      Object key,
      com.jetbrains.youtrack.db.internal.core.id.RID value,
      IndexEngineValidator<Object, com.jetbrains.youtrack.db.internal.core.id.RID> validator) {
    throw new UnsupportedOperationException(
        "Validated put is not supported by LuceneGeoSpatialIndexEngine");
  }

  @Override
  public Document buildDocument(DatabaseSessionInternal session, Object key,
      Identifiable value) {
    EntityImpl location = ((Identifiable) key).getRecord();
    return newGeoDocument(value, factory.fromDoc(location), location);
  }

  @Override
  public boolean isLegacy() {
    return false;
  }

  @Override
  public void updateUniqueIndexVersion(Object key) {
    // not implemented
  }

  @Override
  public int getUniqueIndexVersion(Object key) {
    return 0; // not implemented
  }
}
