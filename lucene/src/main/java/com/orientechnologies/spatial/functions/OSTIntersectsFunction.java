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
package com.orientechnologies.spatial.functions;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLFromClause;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderIntersects;
import org.locationtech.spatial4j.shape.Shape;

/**
 *
 */
public class OSTIntersectsFunction extends OSpatialFunctionAbstractIndexable {

  public static final String NAME = "st_intersects";

  public OSTIntersectsFunction() {
    super(NAME, 2, 2);
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      CommandContext iContext) {

    if (containsNull(iParams)) {
      return null;
    }
    Shape shape = factory.fromObject(iParams[0]);

    Shape shape1 = factory.fromObject(iParams[1]);

    return factory.operation().intersect(shape, shape1);
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return null;
  }

  @Override
  public Iterable<YTIdentifiable> searchFromTarget(
      SQLFromClause target,
      SQLBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      SQLExpression... args) {
    return results(target, args, ctx, rightValue);
  }

  @Override
  protected String operator() {
    return SpatialQueryBuilderIntersects.NAME;
  }
}
