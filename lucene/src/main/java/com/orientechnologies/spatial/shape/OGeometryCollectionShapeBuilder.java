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
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;

/**
 *
 */
public class OGeometryCollectionShapeBuilder extends OComplexShapeBuilder<ShapeCollection<Shape>> {

  protected OShapeFactory shapeFactory;

  public OGeometryCollectionShapeBuilder(OShapeFactory shapeFactory) {
    this.shapeFactory = shapeFactory;
  }

  @Override
  public String getName() {
    return "OGeometryCollection";
  }

  @Override
  public OShapeType getType() {
    return OShapeType.GEOMETRYCOLLECTION;
  }

  @Override
  public ShapeCollection<Shape> fromMapGeoJson(Map<String, Object> geoJsonMap) {
    YTDocument doc = new YTDocument(getName());
    doc.field("geometries", geoJsonMap.get("geometries"));
    return fromDoc(doc);
  }

  @Override
  public ShapeCollection<Shape> fromDoc(YTDocument doc) {

    List<Object> geometries = doc.field("geometries");

    List<Shape> shapes = new ArrayList<Shape>();

    for (Object geometry : geometries) {
      Shape shape = shapeFactory.fromObject(geometry);
      shapes.add(shape);
    }

    return new ShapeCollection(shapes, SPATIAL_CONTEXT);
  }

  @Override
  public void initClazz(YTDatabaseSessionInternal db) {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass shape = superClass(db);
    YTClass polygon = schema.createAbstractClass(getName(), shape);
    polygon.createProperty(db, "geometries", YTType.EMBEDDEDLIST, shape);
  }

  @Override
  public String asText(ShapeCollection<Shape> shapes) {

    Geometry[] geometries = new Geometry[shapes.size()];
    int i = 0;
    for (Shape shape : shapes) {
      geometries[i] = SPATIAL_CONTEXT.getGeometryFrom(shape);
      i++;
    }
    return GEOMETRY_FACTORY.createGeometryCollection(geometries).toText();
  }

  @Override
  public YTDocument toDoc(ShapeCollection<Shape> shapes) {

    YTDocument doc = new YTDocument(getName());
    List<YTDocument> geometries = new ArrayList<YTDocument>(shapes.size());
    for (Shape s : shapes) {
      geometries.add(shapeFactory.toDoc(s));
    }
    doc.field("geometries", geometries);
    return doc;
  }
}
