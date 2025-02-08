package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.EmptyEntity;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.EntityWithEmbeddedCollections;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.EntityWithPrimitiveProperties;
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

  @Test
  public void registerEntityWithEmbeddedCollections() {
    var schema = db.getSchema();

    var result = schema.registerMaterializedEntity(EntityWithEmbeddedCollections.class);

    assertEquals(0, result.getAllSuperClasses().size());
    assertEquals(EntityWithEmbeddedCollections.class.getSimpleName(), result.getName());
    assertEquals(EntityWithEmbeddedCollections.class, result.getMaterializedEntity());

    var properties = result.propertiesMap(db);

    assertEquals(3, properties.size());
    assertEquals(PropertyType.EMBEDDEDLIST, properties.get("stringList").getType());
    assertEquals(PropertyType.STRING, properties.get("stringList").getLinkedType());

    assertEquals(PropertyType.EMBEDDEDSET, properties.get("stringSet").getType());
    assertEquals(PropertyType.STRING, properties.get("stringSet").getLinkedType());

    assertEquals(PropertyType.EMBEDDEDMAP, properties.get("integerMap").getType());
    assertEquals(PropertyType.INTEGER, properties.get("integerMap").getLinkedType());
  }
}
