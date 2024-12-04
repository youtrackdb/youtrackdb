package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.YTSchemaException;
import org.junit.Test;

public class YTSchemaSharedGlobalPropertyTest extends DBTestBase {

  @Test
  public void testGlobalPropertyCreate() {

    YTSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("testaasd", YTType.SHORT, 100);
    OGlobalProperty prop = schema.getGlobalPropertyById(100);
    assertEquals(prop.getName(), "testaasd");
    assertEquals(prop.getId(), (Integer) 100);
    assertEquals(prop.getType(), YTType.SHORT);
  }

  @Test
  public void testGlobalPropertyCreateDoubleSame() {

    YTSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", YTType.SHORT, 200);
    schema.createGlobalProperty("test", YTType.SHORT, 200);
  }

  @Test(expected = YTSchemaException.class)
  public void testGlobalPropertyCreateDouble() {

    YTSchema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", YTType.SHORT, 201);
    schema.createGlobalProperty("test1", YTType.SHORT, 201);
  }
}
