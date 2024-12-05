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

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class OLineStringShapeBuilder extends OComplexShapeBuilder<JtsGeometry> {

  @Override
  public String getName() {
    return "OLineString";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.LINESTRING;
  }

  @Override
  public void initClazz(YTDatabaseSessionInternal db) {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass lineString = schema.createAbstractClass(getName(), superClass(db));
    lineString.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST, YTType.EMBEDDEDLIST);

    if (GlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean()) {
      YTClass lineStringZ = schema.createAbstractClass(getName() + "Z", superClass(db));
      lineStringZ.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST, YTType.EMBEDDEDLIST);
    }
  }

  @Override
  public JtsGeometry fromDoc(EntityImpl document) {

    validate(document);
    List<List<Number>> coordinates = document.field(COORDINATES);

    Coordinate[] coords = new Coordinate[coordinates.size()];
    int i = 0;
    for (List<Number> coordinate : coordinates) {
      coords[i] = new Coordinate(coordinate.get(0).doubleValue(), coordinate.get(1).doubleValue());
      i++;
    }
    return toShape(GEOMETRY_FACTORY.createLineString(coords));
  }

  @Override
  public EntityImpl toDoc(JtsGeometry shape) {
    EntityImpl doc = new EntityImpl(getName());
    LineString lineString = (LineString) shape.getGeom();
    doc.field(COORDINATES, coordinatesFromLineString(lineString));
    return doc;
  }

  @Override
  protected EntityImpl toDoc(JtsGeometry shape, Geometry geometry) {
    if (geometry == null || Double.isNaN(geometry.getCoordinate().getZ())) {
      return toDoc(shape);
    }

    EntityImpl doc = new EntityImpl(getName() + "Z");
    doc.field(COORDINATES, coordinatesFromLineStringZ(geometry));
    return doc;
  }

  @Override
  public String asText(EntityImpl document) {
    if (document.getClassName().equals("OLineStringZ")) {
      List<List<Double>> coordinates = document.getProperty("coordinates");

      String result =
          coordinates.stream()
              .map(
                  point ->
                      (point.stream().map(coord -> format(coord)).collect(Collectors.joining(" "))))
              .collect(Collectors.joining(", "));
      return "LINESTRING Z (" + result + ")";

    } else {
      return super.asText(document);
    }
  }
}
