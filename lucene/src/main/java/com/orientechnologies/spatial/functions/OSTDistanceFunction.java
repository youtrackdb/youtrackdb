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
import com.orientechnologies.spatial.shape.OShapeFactory;
import org.locationtech.spatial4j.shape.Shape;

/**
 *
 */
public class OSTDistanceFunction extends OSpatialFunctionAbstract {

  public static final String NAME = "st_distance";
  private final OShapeFactory factory = OShapeFactory.INSTANCE;

  public OSTDistanceFunction() {
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

    Shape shape = toShape(iParams[0]);
    Shape shape1 = toShape(iParams[1]);

    if (shape == null || shape1 == null) {
      return null;
    }

    return factory.operation().distance(shape, shape1);
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return null;
  }
}
