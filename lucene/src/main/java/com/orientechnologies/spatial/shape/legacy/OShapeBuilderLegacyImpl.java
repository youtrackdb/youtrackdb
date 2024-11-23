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
package com.orientechnologies.spatial.shape.legacy;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.index.OCompositeKey;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.shape.Shape;

/**
 *
 */
public class OShapeBuilderLegacyImpl implements OShapeBuilderLegacy<Shape> {

  public static final OShapeBuilderLegacyImpl INSTANCE = new OShapeBuilderLegacyImpl();
  private final List<OShapeBuilderLegacy> builders = new ArrayList<OShapeBuilderLegacy>();

  protected OShapeBuilderLegacyImpl() {
    builders.add(new OPointLegecyBuilder());
    builders.add(new ORectangleLegacyBuilder());
  }

  @Override
  public Shape makeShape(ODatabaseSessionInternal session, OCompositeKey key, SpatialContext ctx) {
    for (OShapeBuilderLegacy f : builders) {
      if (f.canHandle(key)) {
        return f.makeShape(session, key, ctx);
      }
    }
    return null;
  }

  @Override
  public boolean canHandle(OCompositeKey key) {
    return false;
  }
}
