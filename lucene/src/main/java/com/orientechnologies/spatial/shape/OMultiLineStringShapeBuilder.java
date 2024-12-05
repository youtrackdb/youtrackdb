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

import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class OMultiLineStringShapeBuilder extends OComplexShapeBuilder<JtsGeometry> {

  @Override
  public String getName() {
    return "OMultiLineString";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.MULTILINESTRING;
  }

  @Override
  public JtsGeometry fromDoc(YTEntityImpl document) {
    validate(document);
    List<List<List<Number>>> coordinates = document.field(COORDINATES);
    LineString[] multiLine = new LineString[coordinates.size()];
    int j = 0;
    for (List<List<Number>> coordinate : coordinates) {
      multiLine[j] = createLineString(coordinate);
      j++;
    }
    return toShape(GEOMETRY_FACTORY.createMultiLineString(multiLine));
  }

  @Override
  public void initClazz(YTDatabaseSessionInternal db) {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass lineString = schema.createAbstractClass(getName(), superClass(db));
    lineString.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST, YTType.EMBEDDEDLIST);

    if (YTGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean()) {
      YTClass lineStringZ = schema.createAbstractClass(getName() + "Z", superClass(db));
      lineStringZ.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST, YTType.EMBEDDEDLIST);
    }
  }

  @Override
  public YTEntityImpl toDoc(JtsGeometry shape) {
    final MultiLineString geom = (MultiLineString) shape.getGeom();

    List<List<List<Double>>> coordinates = new ArrayList<List<List<Double>>>();
    YTEntityImpl doc = new YTEntityImpl(getName());
    for (int i = 0; i < geom.getNumGeometries(); i++) {
      final LineString lineString = (LineString) geom.getGeometryN(i);
      coordinates.add(coordinatesFromLineString(lineString));
    }

    doc.field(COORDINATES, coordinates);
    return doc;
  }

  @Override
  protected YTEntityImpl toDoc(JtsGeometry shape, Geometry geometry) {
    if (geometry == null || Double.isNaN(geometry.getCoordinates()[0].getZ())) {
      return toDoc(shape);
    }

    List<List<List<Double>>> coordinates = new ArrayList<List<List<Double>>>();
    YTEntityImpl doc = new YTEntityImpl(getName() + "Z");
    for (int i = 0; i < geometry.getNumGeometries(); i++) {
      final Geometry lineString = geometry.getGeometryN(i);
      coordinates.add(coordinatesFromLineStringZ(lineString));
    }

    doc.field(COORDINATES, coordinates);
    return doc;
  }

  @Override
  public String asText(YTEntityImpl document) {
    if (document.getClassName().equals("OMultiLineStringZ")) {
      List<List<List<Double>>> coordinates = document.getProperty("coordinates");

      String result =
          coordinates.stream()
              .map(
                  line ->
                      "("
                          + line.stream()
                          .map(
                              point ->
                                  (point.stream()
                                      .map(coord -> format(coord))
                                      .collect(Collectors.joining(" "))))
                          .collect(Collectors.joining(", "))
                          + ")")
              .collect(Collectors.joining(", "));
      return "MULTILINESTRING Z(" + result + ")";

    } else {
      return super.asText(document);
    }
  }
}
