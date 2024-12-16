package com.jetbrains.youtrack.db.internal.spatial.functions;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.spatial.BaseSpatialLuceneTest;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.io.WKTReader;

public class STGeomFromTextFunctionTest extends BaseSpatialLuceneTest {

  protected static final WKTReader wktReader = new WKTReader();

  @Test
  public void test() {
    boolean prevValue = GlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean();
    GlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(true);
    try {
      STGeomFromTextFunction func = new STGeomFromTextFunction();
      EntityImpl item =
          (EntityImpl) func.execute(null, null, null, new Object[]{"POINT (100.0 80.0)"}, null);
      Assert.assertEquals("OPoint", item.getClassName());
      Assert.assertEquals(2, ((List) item.getProperty("coordinates")).size());

      item =
          (EntityImpl) func.execute(null, null, null, new Object[]{"POINT Z(100.0 80.0 10)"},
              null);
      Assert.assertEquals("OPointZ", item.getClassName());
      Assert.assertEquals(3, ((List) item.getProperty("coordinates")).size());

      item =
          (EntityImpl)
              func.execute(
                  null,
                  null,
                  null,
                  new Object[]{"LINESTRING Z (1 1 0, 1 2 0, 1 3 1, 2 2 0)"},
                  null);
      Assert.assertEquals("OLineStringZ", item.getClassName());
      Assert.assertEquals(3, ((List<List<Double>>) item.getProperty("coordinates")).get(0).size());
      Assert.assertFalse(
          Double.isNaN(((List<List<Double>>) item.getProperty("coordinates")).get(0).get(2)));

      item =
          (EntityImpl)
              func.execute(
                  null,
                  null,
                  null,
                  new Object[]{"POLYGON Z ((0 0 1, 0 1 0, 1 1 0, 1 0 0, 0 0 0))"},
                  null);
      Assert.assertEquals("OPolygonZ", item.getClassName());
      Assert.assertEquals(
          5, ((List<List<List<Double>>>) item.getProperty("coordinates")).get(0).size());
      Assert.assertFalse(
          Double.isNaN(
              ((List<List<List<Double>>>) item.getProperty("coordinates")).get(0).get(0).get(2)));

      item =
          (EntityImpl)
              func.execute(
                  null,
                  null,
                  null,
                  new Object[]{"MULTILINESTRING Z ((1 1 0, 1 2 0), (1 3 1, 2 2 0))"},
                  null);
      Assert.assertEquals("OMultiLineStringZ", item.getClassName());
      Assert.assertEquals(
          2, ((List<List<List<Double>>>) item.getProperty("coordinates")).get(0).size());
      Assert.assertFalse(
          Double.isNaN(
              ((List<List<List<Double>>>) item.getProperty("coordinates")).get(0).get(0).get(2)));
    } finally {
      GlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(prevValue);
    }
  }
}
