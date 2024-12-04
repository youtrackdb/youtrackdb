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

import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Point;

public class OPointShapeBuilder extends OShapeBuilder<Point> {

  public static final String NAME = "OPoint";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public OShapeType getType() {
    return OShapeType.POINT;
  }

  @Override
  public void initClazz(YTDatabaseSessionInternal db) {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass point = schema.createAbstractClass(NAME, superClass(db));
    YTProperty coordinates = point.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST,
        YTType.DOUBLE);
    coordinates.setMin(db, "2");
    coordinates.setMax(db, "2");

    if (YTGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean()) {
      YTClass pointz = schema.createAbstractClass(NAME + "Z", superClass(db));
      YTProperty coordinatesz = pointz.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST,
          YTType.DOUBLE);
      coordinatesz.setMin(db, "3");
      coordinatesz.setMax(db, "3");
    }
  }

  @Override
  public Point fromDoc(YTDocument document) {
    validate(document);
    List<Number> coordinates = document.field(COORDINATES);
    if (coordinates.size() == 2) {
      return SHAPE_FACTORY.pointXY(
          coordinates.get(0).doubleValue(), coordinates.get(1).doubleValue());
    } else {
      return SHAPE_FACTORY.pointXYZ(
          coordinates.get(0).doubleValue(),
          coordinates.get(1).doubleValue(),
          coordinates.get(2).doubleValue());
    }
  }

  @Override
  public YTDocument toDoc(final Point shape) {

    YTDocument doc = new YTDocument(NAME);
    doc.field(
        COORDINATES,
        new ArrayList<Double>() {
          {
            add(shape.getX());
            add(shape.getY());
          }
        });
    return doc;
  }

  @Override
  protected YTDocument toDoc(Point parsed, Geometry geometry) {
    if (geometry == null || Double.isNaN(geometry.getCoordinate().getZ())) {
      return toDoc(parsed);
    }

    YTDocument doc = new YTDocument(NAME + "Z");
    doc.field(
        COORDINATES,
        new ArrayList<Double>() {
          {
            add(geometry.getCoordinate().getX());
            add(geometry.getCoordinate().getY());
            add(geometry.getCoordinate().getZ());
          }
        });
    return doc;
  }

  @Override
  public String asText(YTDocument document) {
    if (document.getClassName().equals("OPointZ")) {
      List<Double> coordinates = document.getProperty("coordinates");
      return "POINT Z ("
          + format(coordinates.get(0))
          + " "
          + format(coordinates.get(1))
          + " "
          + format(coordinates.get(2))
          + ")";
    } else {
      return super.asText(document);
    }
  }
}
