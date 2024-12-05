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
package com.orientechnologies.spatial.index;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.OInvalidIndexEngineIdException;
import com.jetbrains.youtrack.db.internal.core.index.OIndexMetadata;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChangesPerKey;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChangesPerKey.OTransactionIndexEntry;
import com.orientechnologies.lucene.index.OLuceneIndexNotUnique;
import com.orientechnologies.spatial.engine.OLuceneSpatialIndexContainer;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;

public class OLuceneSpatialIndex extends OLuceneIndexNotUnique {

  private final OShapeFactory shapeFactory = OShapeFactory.INSTANCE;

  public OLuceneSpatialIndex(OIndexMetadata im, final Storage storage) {
    super(im, storage);
  }

  @Override
  public OLuceneIndexNotUnique put(YTDatabaseSessionInternal session, Object key,
      YTIdentifiable value) {
    if (key == null) {
      return this;
    }
    return super.put(session, key, value);
  }

  @Override
  public Iterable<OTransactionIndexEntry> interpretTxKeyChanges(
      final OTransactionIndexChangesPerKey changes) {

    try {
      return storage.callIndexEngine(
          false,
          indexId,
          engine -> {
            if (((OLuceneSpatialIndexContainer) engine).isLegacy()) {
              return OLuceneSpatialIndex.super.interpretTxKeyChanges(changes);
            } else {
              return interpretAsSpatial(changes);
            }
          });
    } catch (OInvalidIndexEngineIdException e) {
      e.printStackTrace();
    }

    return super.interpretTxKeyChanges(changes);
  }

  @Override
  protected Object encodeKey(Object key) {

    if (key instanceof EntityImpl) {
      Shape shape = shapeFactory.fromDoc((EntityImpl) key);
      return shapeFactory.toGeometry(shape);
    }
    return key;
  }

  @Override
  protected Object decodeKey(Object key) {

    if (key instanceof Geometry geom) {
      return shapeFactory.toDoc(geom);
    }
    return key;
  }

  private static Iterable<OTransactionIndexEntry> interpretAsSpatial(
      OTransactionIndexChangesPerKey item) {
    // 1. Handle common fast paths.

    List<OTransactionIndexChangesPerKey.OTransactionIndexEntry> entries = item.getEntriesAsList();
    Map<YTIdentifiable, Integer> counters = new LinkedHashMap<>();

    for (OTransactionIndexChangesPerKey.OTransactionIndexEntry entry : entries) {

      Integer counter = counters.get(entry.getValue());
      if (counter == null) {
        counter = 0;
      }
      switch (entry.getOperation()) {
        case PUT:
          counter++;
          break;
        case REMOVE:
          counter--;
          break;
        case CLEAR:
          break;
      }
      counters.put(entry.getValue(), counter);
    }

    OTransactionIndexChangesPerKey changes = new OTransactionIndexChangesPerKey(item.key);

    for (Map.Entry<YTIdentifiable, Integer> entry : counters.entrySet()) {
      YTIdentifiable oIdentifiable = entry.getKey();
      switch (entry.getValue()) {
        case 1:
          changes.add(oIdentifiable, OTransactionIndexChanges.OPERATION.PUT);
          break;
        case -1:
          changes.add(oIdentifiable, OTransactionIndexChanges.OPERATION.REMOVE);
          break;
      }
    }
    return changes.getEntriesAsList();
  }
}
