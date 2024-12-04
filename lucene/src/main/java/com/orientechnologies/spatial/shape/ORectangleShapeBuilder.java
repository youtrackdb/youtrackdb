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
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;

public class ORectangleShapeBuilder extends OShapeBuilder<Rectangle> {

  public static final String NAME = "ORectangle";

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public OShapeType getType() {
    return OShapeType.RECTANGLE;
  }

  @Override
  public void initClazz(YTDatabaseSessionInternal db) {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass rectangle = schema.createAbstractClass(NAME, superClass(db));
    YTProperty coordinates = rectangle.createProperty(db, COORDINATES, YTType.EMBEDDEDLIST,
        YTType.DOUBLE);
    coordinates.setMin(db, "4");
    coordinates.setMin(db, "4");
  }

  @Override
  public Rectangle fromDoc(YTDocument document) {
    validate(document);
    List<Number> coordinates = document.field(COORDINATES);

    Point topLeft =
        SPATIAL_CONTEXT.makePoint(
            coordinates.get(0).doubleValue(), coordinates.get(1).doubleValue());
    Point bottomRight =
        SPATIAL_CONTEXT.makePoint(
            coordinates.get(2).doubleValue(), coordinates.get(3).doubleValue());
    Rectangle rectangle = SPATIAL_CONTEXT.makeRectangle(topLeft, bottomRight);
    return rectangle;
  }

  @Override
  public YTDocument toDoc(final Rectangle shape) {

    YTDocument doc = new YTDocument(NAME);

    doc.field(
        COORDINATES,
        new ArrayList<Double>() {
          {
            add(shape.getMinX());
            add(shape.getMinY());
            add(shape.getMaxX());
            add(shape.getMaxY());
          }
        });
    return doc;
  }
}
