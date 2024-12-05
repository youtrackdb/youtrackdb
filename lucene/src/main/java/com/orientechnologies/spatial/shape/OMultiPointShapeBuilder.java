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
package com.orientechnologies.spatial.shape;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class OMultiPointShapeBuilder extends OComplexShapeBuilder<JtsGeometry> {

  @Override
  public String getName() {
    return "OMultiPoint";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.MULTIPOINT;
  }

  @Override
  public JtsGeometry fromDoc(YTEntityImpl document) {
    validate(document);
    List<List<Number>> coordinates = document.field(COORDINATES);
    Coordinate[] coords = new Coordinate[coordinates.size()];
    int i = 0;
    for (List<Number> coordinate : coordinates) {
      coords[i] = new Coordinate(coordinate.get(0).doubleValue(), coordinate.get(1).doubleValue());
      i++;
    }
    return toShape(GEOMETRY_FACTORY.createMultiPoint(coords));
  }

  @Override
  public void initClazz(YTDatabaseSessionInternal db) {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass multiPoint = schema.createAbstractClass(getName(), superClass(db));
    multiPoint.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST, YTType.EMBEDDEDLIST);
  }

  @Override
  public YTEntityImpl toDoc(final JtsGeometry shape) {
    final MultiPoint geom = (MultiPoint) shape.getGeom();

    YTEntityImpl doc = new YTEntityImpl(getName());
    doc.field(
        COORDINATES,
        new ArrayList<List<Double>>() {
          {
            Coordinate[] coordinates = geom.getCoordinates();
            for (Coordinate coordinate : coordinates) {
              add(Arrays.asList(coordinate.x, coordinate.y));
            }
          }
        });
    return doc;
  }
}
