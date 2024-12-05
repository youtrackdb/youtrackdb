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
import com.jetbrains.youtrack.db.internal.core.sql.parser.OBinaryCompareOperator;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OExpression;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OFromClause;
import com.orientechnologies.spatial.strategy.SpatialQueryBuilderDWithin;
import java.util.Map;
import org.locationtech.spatial4j.shape.Shape;

/**
 *
 */
public class OSTDWithinFunction extends OSpatialFunctionAbstractIndexable {

  public static final String NAME = "st_dwithin";

  public OSTDWithinFunction() {
    super(NAME, 3, 3);
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

    Number distance = (Number) iParams[2];

    return factory.operation().isWithInDistance(shape, shape1, distance.doubleValue());
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return null;
  }

  @Override
  public Iterable<YTIdentifiable> searchFromTarget(
      OFromClause target,
      OBinaryCompareOperator operator,
      Object rightValue,
      CommandContext ctx,
      OExpression... args) {
    return results(target, args, ctx, rightValue);
  }

  @Override
  protected void onAfterParsing(
      Map<String, Object> params, OExpression[] args, CommandContext ctx, Object rightValue) {

    OExpression number = args[2];

    Number parsedNumber = (Number) number.execute((YTIdentifiable) null, ctx);

    params.put("distance", parsedNumber.doubleValue());
  }

  @Override
  protected String operator() {
    return SpatialQueryBuilderDWithin.NAME;
  }
}
