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
package com.orientechnologies.spatial.operator;

import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.DocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.filter.SQLFilterCondition;
import com.orientechnologies.spatial.collections.SpatialCompositeKey;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderAbstract;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderOverlap;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.shape.Shape;

public class LuceneOverlapOperator extends LuceneSpatialOperator {

  public LuceneOverlapOperator() {
    super("&&", 5, false);
  }

  @Override
  public Stream<RawPair<Object, RID>> executeIndexQuery(
      CommandContext iContext, Index index, List<Object> keyParams, boolean ascSortOrder) {
    Object key;
    key = keyParams.get(0);

    Map<String, Object> queryParams = new HashMap<String, Object>();
    queryParams.put(SpatialQueryBuilderAbstract.GEO_FILTER, SpatialQueryBuilderOverlap.NAME);
    queryParams.put(SpatialQueryBuilderAbstract.SHAPE, key);

    //noinspection resource
    return index
        .getInternal()
        .getRids(iContext.getDatabase(), queryParams)
        .map((rid) -> new RawPair<>(new SpatialCompositeKey(keyParams), rid));
  }

  @Override
  public Object evaluateRecord(
      Identifiable iRecord,
      EntityImpl iCurrentResult,
      SQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      CommandContext iContext,
      final DocumentSerializer serializer) {
    Shape shape = factory.fromDoc((EntityImpl) iLeft);

    // TODO { 'shape' : { 'type' : 'LineString' , 'coordinates' : [[1,2],[4,6]]} }
    // TODO is not translated in map but in array[ { 'type' : 'LineString' , 'coordinates' :
    // [[1,2],[4,6]]} ]
    Object filter;
    if (iRight instanceof Collection) {
      filter = ((Collection) iRight).iterator().next();
    } else {
      filter = iRight;
    }
    Shape shape1 = factory.fromObject(filter);

    return SpatialOperation.BBoxIntersects.evaluate(shape, shape1.getBoundingBox());
  }
}
