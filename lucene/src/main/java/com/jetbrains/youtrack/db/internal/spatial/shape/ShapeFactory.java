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
package com.jetbrains.youtrack.db.internal.spatial.shape;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.Result;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.spatial4j.shape.Point;
import org.locationtech.spatial4j.shape.Rectangle;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

public class ShapeFactory extends ComplexShapeBuilder {

  public static final ShapeFactory INSTANCE = new ShapeFactory();

  protected ShapeOperation operation;

  private final Map<String, ShapeBuilder> factories = new HashMap<String, ShapeBuilder>();

  protected ShapeFactory() {
    operation = new ShapeOperationImpl(this);
    registerFactory(new LineStringShapeBuilder());
    registerFactory(new MultiLineStringShapeBuilder());
    registerFactory(new PointShapeBuilder());
    registerFactory(new MultiPointShapeBuilder());
    registerFactory(new RectangleShapeBuilder());
    registerFactory(new PolygonShapeBuilder());
    registerFactory(new MultiPolygonShapeBuilder());
    registerFactory(new GeometryCollectionShapeBuilder(this));
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public OShapeType getType() {
    return null;
  }

  @Override
  public void initClazz(DatabaseSessionInternal db) {
    for (ShapeBuilder f : factories.values()) {
      f.initClazz(db);
    }
  }

  @Override
  public Shape fromDoc(EntityImpl document) {
    ShapeBuilder shapeBuilder = factories.get(document.getClassName());
    if (shapeBuilder != null) {
      return shapeBuilder.fromDoc(document);
    }
    // TODO handle exception shape not found
    return null;
  }

  @Override
  public Shape fromObject(Object obj) {

    if (obj instanceof String) {
      try {
        return fromText((String) obj);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (obj instanceof EntityImpl) {
      return fromDoc((EntityImpl) obj);
    }
    if (obj instanceof Result) {
      Entity entity = ((Result) obj).toEntity();
      return fromDoc((EntityImpl) entity);
    }
    if (obj instanceof Map) {
      Map map = (Map) ((Map) obj).get("shape");
      if (map == null) {
        map = (Map) obj;
      }
      return fromMapGeoJson(map);
    }
    return null;
  }

  @Override
  public String asText(EntityImpl document) {
    String className = document.getClassName();
    ShapeBuilder shapeBuilder = factories.get(className);
    if (shapeBuilder != null) {
      return shapeBuilder.asText(document);
    } else if (className.endsWith("Z")) {
      shapeBuilder = factories.get(className.substring(0, className.length() - 1));
      if (shapeBuilder != null) {
        return shapeBuilder.asText(document);
      }
    }

    // TODO handle exception shape not found
    return null;
  }

  @Override
  public String asText(Object obj) {

    if (obj instanceof Result) {
      Entity entity = ((Result) obj).toEntity();
      return asText((EntityImpl) entity);
    }
    if (obj instanceof EntityImpl) {
      return asText((EntityImpl) obj);
    }
    if (obj instanceof Map) {
      Map map = (Map) ((Map) obj).get("shape");
      if (map == null) {
        map = (Map) obj;
      }
      return asText(map);
    }
    return null;
  }

  public byte[] asBinary(Object obj) {

    if (obj instanceof EntityImpl) {
      Shape shape = fromDoc((EntityImpl) obj);
      return asBinary(shape);
    }
    if (obj instanceof Map) {
      Map map = (Map) ((Map) obj).get("shape");
      if (map == null) {
        map = (Map) obj;
      }
      Shape shape = fromMapGeoJson(map);

      return asBinary(shape);
    }
    throw new IllegalArgumentException("Error serializing to binary " + obj);
  }

  @Override
  public EntityImpl toEntitty(Shape shape) {

    // TODO REFACTOR
    EntityImpl doc = null;
    if (Point.class.isAssignableFrom(shape.getClass())) {
      doc = factories.get(PointShapeBuilder.NAME).toEntitty(shape);
    } else if (Rectangle.class.isAssignableFrom(shape.getClass())) {
      doc = factories.get(RectangleShapeBuilder.NAME).toEntitty(shape);
    } else if (JtsGeometry.class.isAssignableFrom(shape.getClass())) {
      JtsGeometry geometry = (JtsGeometry) shape;
      Geometry geom = geometry.getGeom();
      doc = factories.get("O" + geom.getClass().getSimpleName()).toEntitty(shape);

    } else if (ShapeCollection.class.isAssignableFrom(shape.getClass())) {
      ShapeCollection collection = (ShapeCollection) shape;

      if (isMultiPolygon(collection)) {
        doc = factories.get("OMultiPolygon").toEntitty(createMultiPolygon(collection));
      } else if (isMultiPoint(collection)) {
        doc = factories.get("OMultiPoint").toEntitty(createMultiPoint(collection));
      } else if (isMultiLine(collection)) {
        doc = factories.get("OMultiLineString").toEntitty(createMultiLine(collection));
      } else {
        doc = factories.get("OGeometryCollection").toEntitty(shape);
      }
    }
    return doc;
  }

  @Override
  protected EntityImpl toEntitty(Shape shape, Geometry geometry) {
    if (Point.class.isAssignableFrom(shape.getClass())) {
      return factories.get(PointShapeBuilder.NAME).toEntitty(shape, geometry);
    } else if (geometry != null && "LineString".equals(geometry.getClass().getSimpleName())) {
      return factories.get("OLineString").toEntitty(shape, geometry);
    } else if (geometry != null && "MultiLineString".equals(geometry.getClass().getSimpleName())) {
      return factories.get("OMultiLineString").toEntitty(shape, geometry);
    } else if (geometry != null && "Polygon".equals(geometry.getClass().getSimpleName())) {
      return factories.get("OPolygon").toEntitty(shape, geometry);
    } else {
      return toEntitty(shape);
    }
  }

  @Override
  public Shape fromMapGeoJson(Map geoJsonMap) {
    ShapeBuilder shapeBuilder = factories.get(geoJsonMap.get("type"));

    if (shapeBuilder == null) {
      shapeBuilder = factories.get(geoJsonMap.get("@class"));
    }
    if (shapeBuilder != null) {
      return shapeBuilder.fromMapGeoJson(geoJsonMap);
    }
    throw new IllegalArgumentException("Invalid map");
    // TODO handle exception shape not found
  }

  public Geometry toGeometry(Shape shape) {
    if (shape instanceof ShapeCollection) {
      ShapeCollection<Shape> shapes = (ShapeCollection<Shape>) shape;
      Geometry[] geometries = new Geometry[shapes.size()];
      int i = 0;
      for (Shape shapeItem : shapes) {
        geometries[i] = SPATIAL_CONTEXT.getGeometryFrom(shapeItem);
        i++;
      }
      return GEOMETRY_FACTORY.createGeometryCollection(geometries);
    } else {
      return SPATIAL_CONTEXT.getGeometryFrom(shape);
    }
  }

  public EntityImpl toEntitty(Geometry geometry) {
    if (geometry instanceof org.locationtech.jts.geom.Point point) {
      Point point1 = context().makePoint(point.getX(), point.getY());
      return toEntitty(point1);
    }
    if (geometry instanceof org.locationtech.jts.geom.GeometryCollection gc) {
      List<Shape> shapes = new ArrayList<Shape>();
      for (int i = 0; i < gc.getNumGeometries(); i++) {
        Geometry geo = gc.getGeometryN(i);
        Shape shape = null;
        if (geo instanceof org.locationtech.jts.geom.Point point) {
          shape = context().makePoint(point.getX(), point.getY());
        } else {
          shape = SPATIAL_CONTEXT.makeShape(geo);
        }
        shapes.add(shape);
      }
      return toEntitty(new ShapeCollection<Shape>(shapes, SPATIAL_CONTEXT));
    }
    return toEntitty(SPATIAL_CONTEXT.makeShape(geometry));
  }

  public ShapeOperation operation() {
    return operation;
  }

  public void registerFactory(ShapeBuilder factory) {
    factories.put(factory.getName(), factory);
  }
}
