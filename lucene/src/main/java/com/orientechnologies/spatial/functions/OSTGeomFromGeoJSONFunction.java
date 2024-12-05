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

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.spatial.shape.OShapeFactory;

/**
 *
 */
public class OSTGeomFromGeoJSONFunction extends OSQLFunctionAbstract {

  public static final String NAME = "ST_GeomFromGeoJSON";

  private final OShapeFactory factory = OShapeFactory.INSTANCE;

  public OSTGeomFromGeoJSONFunction() {
    super(NAME, 1, 1);
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {
    String geom = (String) iParams[0];
    try {
      return factory.fromGeoJson(geom);
    } catch (Exception e) {
      throw YTException.wrapException(
          new YTCommandExecutionException(String.format("Cannot parse geometry {%s}", geom)), e);
    }
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return null;
  }
}
