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

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.sql.functions.OSQLFunctionAbstract;
import com.orientechnologies.spatial.shape.OShapeFactory;
import java.util.Map;
import org.locationtech.spatial4j.shape.Shape;

/**
 *
 */
public class OSTBufferFunction extends OSQLFunctionAbstract {

  public static final String NAME = "ST_Buffer";

  private final OShapeFactory factory = OShapeFactory.INSTANCE;

  public OSTBufferFunction() {
    super(NAME, 2, 3);
  }

  @Override
  public Object execute(
      Object iThis,
      YTIdentifiable iCurrentRecord,
      Object iCurrentResult,
      Object[] iParams,
      OCommandContext iContext) {
    Shape shape = factory.fromObject(iParams[0]);
    Number distance = (Number) iParams[1];
    Map params = null;
    if (iParams.length > 2) {
      params = (Map) iParams[2];
    }
    Shape buffer = factory.buffer(shape, distance.doubleValue(), params);
    return factory.toDoc(buffer);
  }

  @Override
  public String getSyntax(YTDatabaseSession session) {
    return "ST_AsBuffer(<doc>)";
  }
}
