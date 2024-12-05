package com.orientechnologies.spatial.functions;

import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.spatial.BaseSpatialLuceneTest;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.locationtech.jts.io.WKTReader;

public class OSTGeomFromTextFunctionTest extends BaseSpatialLuceneTest {

  protected static final WKTReader wktReader = new WKTReader();

  @Test
  public void test() {
    boolean prevValue = YTGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.getValueAsBoolean();
    YTGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(true);
    try {
      OSTGeomFromTextFunction func = new OSTGeomFromTextFunction();
      YTEntityImpl item =
          (YTEntityImpl) func.execute(null, null, null, new Object[]{"POINT (100.0 80.0)"}, null);
      Assert.assertEquals("OPoint", item.getClassName());
      Assert.assertEquals(2, ((List) item.getProperty("coordinates")).size());

      item =
          (YTEntityImpl) func.execute(null, null, null, new Object[]{"POINT Z(100.0 80.0 10)"},
              null);
      Assert.assertEquals("OPointZ", item.getClassName());
      Assert.assertEquals(3, ((List) item.getProperty("coordinates")).size());

      item =
          (YTEntityImpl)
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
          (YTEntityImpl)
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
          (YTEntityImpl)
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
      YTGlobalConfiguration.SPATIAL_ENABLE_DIRECT_WKT_READER.setValue(prevValue);
    }
  }
}
