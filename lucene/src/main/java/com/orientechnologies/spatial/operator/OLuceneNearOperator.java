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

import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.orientechnologies.lucene.operator.OLuceneOperatorUtil;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.OIndexSearchResult;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OIndexReuseType;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryTargetOperator;
import com.orientechnologies.spatial.collections.OSpatialCompositeKey;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.shape.Circle;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class OLuceneNearOperator extends OQueryTargetOperator {

  private final OShapeFactory factory = OShapeFactory.INSTANCE;

  public OLuceneNearOperator() {
    super("NEAR", 5, false);
  }

  @Override
  public Object evaluateRecord(
      YTIdentifiable iRecord,
      EntityImpl iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      OCommandContext iContext,
      final ODocumentSerializer serializer) {

    List<Number> left = (List<Number>) iLeft;

    double lat = left.get(0).doubleValue();
    double lon = left.get(1).doubleValue();

    Shape shape = factory.context().makePoint(lon, lat);
    List<Number> right = (List<Number>) iRight;

    double lat1 = right.get(0).doubleValue();
    double lon1 = right.get(1).doubleValue();
    Shape shape1 = factory.context().makePoint(lon1, lat1);

    Map map = (Map) right.get(2);
    double distance = 0;

    Number n = (Number) map.get("maxDistance");
    if (n != null) {
      distance = n.doubleValue();
    }
    Point p = (Point) shape1;
    Circle circle =
        factory
            .context()
            .makeCircle(
                p.getX(),
                p.getY(),
                DistanceUtils.dist2Degrees(distance, DistanceUtils.EARTH_MEAN_RADIUS_KM));
    double docDistDEG = factory.context().getDistCalc().distance((Point) shape, p);
    final double docDistInKM =
        DistanceUtils.degrees2Dist(docDistDEG, DistanceUtils.EARTH_EQUATORIAL_RADIUS_KM);
    iContext.setVariable("distance", docDistInKM);
    return shape.relate(circle) == SpatialRelation.WITHIN;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      OCommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {

    double distance = 0;
    Object spatial = iContext.getVariable("spatial");
    if (spatial != null) {
      if (spatial instanceof Number) {
        distance = YTType.convert(iContext.getDatabase(), spatial,
            Double.class).doubleValue();
      } else if (spatial instanceof Map) {
        Map<String, Object> params = (Map<String, Object>) spatial;

        Object dst = params.get("maxDistance");
        if (dst != null && dst instanceof Number) {
          distance = YTType.convert(iContext.getDatabase(), dst,
              Double.class).doubleValue();
        }
      }
    }

    iContext.setVariable("$luceneIndex", true);

    return index
        .getInternal()
        .getRids(iContext.getDatabase(),
            new OSpatialCompositeKey(keyParams).setMaxDistance(distance).setContext(iContext))
        .map((rid) -> new ORawPair<>(new OSpatialCompositeKey(keyParams), rid));
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public YTRID getBeginRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public YTRID getEndRidRange(YTDatabaseSession session, Object iLeft, Object iRight) {
    return null;
  }

  @Override
  public String getSyntax() {
    return "<left> NEAR[(<begin-deep-level> [,<maximum-deep-level> [,<fields>]] )] ( <conditions>"
        + " )";
  }

  @Override
  public OIndexSearchResult getOIndexSearchResult(
      YTClass iSchemaClass,
      OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults,
      OCommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(
        iSchemaClass, iCondition, iIndexSearchResults, context);
  }
}
