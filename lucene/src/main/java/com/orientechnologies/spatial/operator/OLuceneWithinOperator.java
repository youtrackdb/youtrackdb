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
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.ODocumentSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.OIndexSearchResult;
import com.jetbrains.youtrack.db.internal.core.sql.filter.OSQLFilterCondition;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OIndexReuseType;
import com.jetbrains.youtrack.db.internal.core.sql.operator.OQueryTargetOperator;
import com.orientechnologies.lucene.operator.OLuceneOperatorUtil;
import com.orientechnologies.spatial.collections.OSpatialCompositeKey;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacy;
import com.orientechnologies.spatial.shape.legacy.OShapeBuilderLegacyImpl;
import java.util.List;
import java.util.stream.Stream;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.SpatialRelation;

public class OLuceneWithinOperator extends OQueryTargetOperator {

  private final OShapeBuilderLegacy<Shape> shapeFactory = OShapeBuilderLegacyImpl.INSTANCE;

  public OLuceneWithinOperator() {
    super("WITHIN", 5, false);
  }

  @Override
  public Object evaluateRecord(
      YTIdentifiable iRecord,
      EntityImpl iCurrentResult,
      OSQLFilterCondition iCondition,
      Object iLeft,
      Object iRight,
      CommandContext iContext,
      final ODocumentSerializer serializer) {
    List<Number> left = (List<Number>) iLeft;

    double lat = left.get(0).doubleValue();
    double lon = left.get(1).doubleValue();

    Shape shape = SpatialContext.GEO.makePoint(lon, lat);

    Shape shape1 =
        shapeFactory.makeShape(iContext.getDatabase(), new OSpatialCompositeKey((List<?>) iRight),
            SpatialContext.GEO);

    return shape.relate(shape1) == SpatialRelation.WITHIN;
  }

  @Override
  public OIndexReuseType getIndexReuseType(Object iLeft, Object iRight) {
    return OIndexReuseType.INDEX_OPERATOR;
  }

  @Override
  public Stream<ORawPair<Object, YTRID>> executeIndexQuery(
      CommandContext iContext, OIndex index, List<Object> keyParams, boolean ascSortOrder) {
    iContext.setVariable("$luceneIndex", true);
    return index
        .getInternal()
        .getRids(iContext.getDatabase(),
            new OSpatialCompositeKey(keyParams).setOperation(SpatialOperation.IsWithin)
                .setContext(iContext))
        .map(
            (rid) -> new ORawPair<>(new OSpatialCompositeKey(keyParams).setContext(iContext), rid));
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
  public OIndexSearchResult getOIndexSearchResult(
      YTClass iSchemaClass,
      OSQLFilterCondition iCondition,
      List<OIndexSearchResult> iIndexSearchResults,
      CommandContext context) {
    return OLuceneOperatorUtil.buildOIndexSearchResult(
        iSchemaClass, iCondition, iIndexSearchResults, context);
  }
}
