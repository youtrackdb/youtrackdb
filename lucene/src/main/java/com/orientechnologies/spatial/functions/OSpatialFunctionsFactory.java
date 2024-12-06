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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.functions.SQLFunctionFactoryTemplate;

public class OSpatialFunctionsFactory extends SQLFunctionFactoryTemplate {

  public OSpatialFunctionsFactory() {
  }

  @Override
  public void registerDefaultFunctions(DatabaseSessionInternal session) {
    register(session, new OSTGeomFromTextFunction());
    register(session, new OSTAsTextFunction());
    register(session, new OSTWithinFunction());
    register(session, new OSTDWithinFunction());
    register(session, new OSTEqualsFunction());
    register(session, new OSTAsBinaryFunction());
    register(session, new OSTEnvelopFunction());
    register(session, new OSTBufferFunction());
    register(session, new OSTDistanceFunction());
    register(session, new OSTDistanceSphereFunction());
    register(session, new OSTDisjointFunction());
    register(session, new OSTIntersectsFunction());
    register(session, new OSTContainsFunction());
    register(session, new OSTSrid());
    register(session, new OSTAsGeoJSONFunction());
    register(session, new OSTGeomFromGeoJSONFunction());
  }
}
