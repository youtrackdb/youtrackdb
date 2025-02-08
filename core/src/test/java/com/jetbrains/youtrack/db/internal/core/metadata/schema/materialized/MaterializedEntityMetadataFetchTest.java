package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Test;

public class MaterializedEntityMetadataFetchTest extends DbTestBase {

  @Test
  public void registerEmptyEntity() {
    var schema = db.getSchema();
    var result = schema.registerMaterializedEntity(EmptyEntity.class);

    assertEquals(0, result.properties(db).size());
    assertEquals(0, result.getAllSuperClasses().size());
    assertEquals(EmptyEntity.class.getSimpleName(), result.getName());
    assertEquals(EmptyEntity.class, result.getMaterializedEntity());
  }

  @Test
  public void registerEntityWithPrimitiveProperties() {
    var schema = db.getSchema();

    var result = schema.registerMaterializedEntity(EntityWithPrimitiveProperties.class);

    assertEquals(7, result.properties(db).size());
    assertEquals(0, result.getAllSuperClasses().size());
    assertEquals(EntityWithPrimitiveProperties.class.getSimpleName(), result.getName());
    assertEquals(EntityWithPrimitiveProperties.class, result.getMaterializedEntity());

    var properties = result.propertiesMap(db);

    assertEquals(PropertyType.INTEGER, properties.get("intProperty").getType());
    assertEquals(PropertyType.LONG, properties.get("longProperty").getType());
    assertEquals(PropertyType.DOUBLE, properties.get("doubleProperty").getType());
    assertEquals(PropertyType.FLOAT, properties.get("floatProperty").getType());
    assertEquals(PropertyType.BOOLEAN, properties.get("booleanProperty").getType());
    assertEquals(PropertyType.BYTE, properties.get("byteProperty").getType());
    assertEquals(PropertyType.SHORT, properties.get("shortProperty").getType());
  }

  interface EmptyEntity extends MaterializedEntity {

  }

  interface EntityWithPrimitiveProperties extends MaterializedEntity {

    int getIntProperty();

    void setIntProperty(int value);

    long getLongProperty();

    void setLongProperty(long value);

    double getDoubleProperty();

    void setDoubleProperty(double value);

    float getFloatProperty();

    void setFloatProperty(float value);

    boolean getBooleanProperty();

    void setBooleanProperty(boolean value);

    byte getByteProperty();

    void setByteProperty(byte value);

    short getShortProperty();

    void setShortProperty(short value);
  }
}
