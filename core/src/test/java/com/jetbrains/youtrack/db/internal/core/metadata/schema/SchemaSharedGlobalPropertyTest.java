package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import org.junit.Test;

public class SchemaSharedGlobalPropertyTest extends DbTestBase {

  @Test
  public void testGlobalPropertyCreate() {

    Schema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("testaasd", PropertyType.SHORT, 100);
    GlobalProperty prop = schema.getGlobalPropertyById(100);
    assertEquals(prop.getName(), "testaasd");
    assertEquals(prop.getId(), (Integer) 100);
    assertEquals(prop.getType(), PropertyType.SHORT);
  }

  @Test
  public void testGlobalPropertyCreateDoubleSame() {

    Schema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", PropertyType.SHORT, 200);
    schema.createGlobalProperty("test", PropertyType.SHORT, 200);
  }

  @Test(expected = SchemaException.class)
  public void testGlobalPropertyCreateDouble() {

    Schema schema = db.getMetadata().getSchema();

    schema.createGlobalProperty("test", PropertyType.SHORT, 201);
    schema.createGlobalProperty("test1", PropertyType.SHORT, 201);
  }
}
