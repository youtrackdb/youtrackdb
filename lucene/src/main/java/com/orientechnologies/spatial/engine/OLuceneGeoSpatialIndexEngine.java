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

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTContextualRecordId;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.OIndexKeyUpdater;
import com.jetbrains.youtrack.db.internal.core.index.YTIndexEngineException;
import com.jetbrains.youtrack.db.internal.core.index.engine.IndexEngineValidator;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
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

public class OLuceneGeoSpatialIndexEngine extends OLuceneSpatialIndexEngineAbstract {

  public OLuceneGeoSpatialIndexEngine(
      Storage storage, String name, int id, OShapeBuilder factory) {
    super(storage, name, id, factory);
  }

  @Override
  protected SpatialStrategy createSpatialStrategy(
      OIndexDefinition indexDefinition, Map<String, ?> metadata) {

    return OSpatialStrategyFactory.createStrategy(ctx, getDatabase(), indexDefinition);
  }

  @Override
  public Object get(YTDatabaseSessionInternal session, Object key) {
    return getInTx(session, key, null);
  }

  @Override
  public Set<YTIdentifiable> getInTx(YTDatabaseSessionInternal session, Object key,
      OLuceneTxChanges changes) {
    updateLastAccess();
    openIfClosed();
    try {
      if (key instanceof Map) {
        //noinspection unchecked
        return newGeoSearch((Map<String, Object>) key, changes);
      }
    } catch (Exception e) {
      if (e instanceof YTException forward) {
        throw forward;
      } else {
        throw YTException.wrapException(new YTIndexEngineException("Error parsing lucene query"),
            e);
      }
    }

    return new OLuceneResultSetEmpty();
  }

  private Set<YTIdentifiable> newGeoSearch(Map<String, Object> key, OLuceneTxChanges changes)
      throws Exception {

    OLuceneQueryContext queryContext = queryStrategy.build(key).withChanges(changes);
    return new OLuceneResultSet(this, queryContext, EMPTY_METADATA);
  }

  @Override
  public void onRecordAddedToResultSet(
      OLuceneQueryContext queryContext,
      YTContextualRecordId recordId,
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
  public void put(YTDatabaseSessionInternal session, OAtomicOperation atomicOperation, Object key,
      Object value) {

    if (key instanceof YTIdentifiable) {
      openIfClosed();
      EntityImpl location = ((YTIdentifiable) key).getRecord();
      updateLastAccess();
      addDocument(newGeoDocument((YTIdentifiable) value, factory.fromDoc(location), location));
    }
  }

  @Override
  public void update(
      YTDatabaseSessionInternal session, OAtomicOperation atomicOperation, Object key,
      OIndexKeyUpdater<Object> updater) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean validatedPut(
      OAtomicOperation atomicOperation,
      Object key,
      YTRID value,
      IndexEngineValidator<Object, YTRID> validator) {
    throw new UnsupportedOperationException(
        "Validated put is not supported by OLuceneGeoSpatialIndexEngine");
  }

  @Override
  public Document buildDocument(YTDatabaseSessionInternal session, Object key,
      YTIdentifiable value) {
    EntityImpl location = ((YTIdentifiable) key).getRecord();
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
