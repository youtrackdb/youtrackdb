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

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
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
  public void initClazz(YTDatabaseSessionInternal db) {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass polygon = schema.createAbstractClass(getName(), superClass(db));
    polygon.createProperty(db, "coordinates", YTType.EMBEDDEDLIST, YTType.EMBEDDEDLIST);
  }

  @Override
  public JtsGeometry fromDoc(YTDocument document) {
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
  public YTDocument toDoc(JtsGeometry shape) {

    YTDocument doc = new YTDocument(getName());
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
