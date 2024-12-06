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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

/**
 *
 */
public class OMultiPolygonShapeBuilder extends OPolygonShapeBuilder {

  @Override
  public String getName() {
    return "OMultiPolygon";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.MULTIPOLYGON;
  }

  @Override
  public void initClazz(DatabaseSessionInternal db) {

    Schema schema = db.getMetadata().getSchema();
    SchemaClass polygon = schema.createAbstractClass(getName(), superClass(db));
    polygon.createProperty(db, "coordinates", PropertyType.EMBEDDEDLIST, PropertyType.EMBEDDEDLIST);
  }

  @Override
  public JtsGeometry fromDoc(EntityImpl document) {
    validate(document);
    List<List<List<List<Number>>>> coordinates = document.field("coordinates");

    Polygon[] polygons = new Polygon[coordinates.size()];
    int i = 0;
    for (List<List<List<Number>>> coordinate : coordinates) {
      polygons[i] = createPolygon(coordinate);
      i++;
    }
    return toShape(GEOMETRY_FACTORY.createMultiPolygon(polygons));
  }

  @Override
  public EntityImpl toDoc(JtsGeometry shape) {

    EntityImpl doc = new EntityImpl(getName());
    MultiPolygon multiPolygon = (MultiPolygon) shape.getGeom();
    List<List<List<List<Double>>>> polyCoordinates = new ArrayList<List<List<List<Double>>>>();
    int n = multiPolygon.getNumGeometries();

    for (int i = 0; i < n; i++) {
      Geometry geom = multiPolygon.getGeometryN(i);
      polyCoordinates.add(coordinatesFromPolygon((Polygon) geom));
    }

    doc.field(COORDINATES, polyCoordinates);
    return doc;
  }
}
