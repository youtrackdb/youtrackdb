package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.CyclicGraphAEntity;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.CyclicGraphBEntity;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.CyclicGraphCEntity;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.EmptyEntity;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.EntityWithEmbeddedCollections;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.EntityWithLinkProperties;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.EntityWithSingleValueProperties;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.MultiInheritanceEntity;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities.SelfReferencedEntity;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;

public class MaterializedEntityMetadataFetchTest extends DbTestBase {

  @Test
  public void registerEmptyEntity() {
    var schema = db.getSchema();
    var result = schema.registerMaterializedEntity(EmptyEntity.class);

    validateEmptyEntity(result);
  }

  private void validateEmptyEntity(SchemaClass result) {
    assertEquals(0, result.properties(db).size());
    assertEquals(0, result.getAllSuperClasses().size());
    assertEquals(EmptyEntity.class.getSimpleName(), result.getName());
    assertEquals(EmptyEntity.class, result.getMaterializedEntity());
  }

  @Test
  public void registerEntityWithSingleValueProperties() {
    var schema = db.getSchema();

    var result = schema.registerMaterializedEntity(EntityWithSingleValueProperties.class);
    validateEntityWithSingleValueProperties(result);
  }

  private void validateEntityWithSingleValueProperties(SchemaClass result) {
    assertEquals(0, result.getAllSuperClasses().size());
    assertEquals(EntityWithSingleValueProperties.class.getSimpleName(), result.getName());
    assertEquals(EntityWithSingleValueProperties.class, result.getMaterializedEntity());

    var properties = result.propertiesMap(db);

    assertEquals(9, properties.size());

    assertEquals(PropertyType.INTEGER, properties.get("intProperty").getType());
    assertEquals(PropertyType.LONG, properties.get("longProperty").getType());
    assertEquals(PropertyType.DOUBLE, properties.get("doubleProperty").getType());
    assertEquals(PropertyType.FLOAT, properties.get("floatProperty").getType());
    assertEquals(PropertyType.BOOLEAN, properties.get("booleanProperty").getType());
    assertEquals(PropertyType.BYTE, properties.get("byteProperty").getType());
    assertEquals(PropertyType.SHORT, properties.get("shortProperty").getType());
    assertEquals(PropertyType.BINARY, properties.get("binaryProperty").getType());
    assertEquals(PropertyType.DATETIME, properties.get("dateProperty").getType());
  }

  @Test
  public void registerEntityWithEmbeddedCollections() {
    var schema = db.getSchema();

    var result = schema.registerMaterializedEntity(EntityWithEmbeddedCollections.class);

    validateEntityWithEmbeddedCollections(result);
  }

  private void validateEntityWithEmbeddedCollections(SchemaClass result) {
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

  @Test
  public void registerEntityWithLinkProperties() {
    var schema = db.getSchema();

    var result = schema.registerMaterializedEntity(EntityWithLinkProperties.class);

    validateEntityWithLinkedProperties(result);
  }

  private void validateEntityWithLinkedProperties(SchemaClass result) {
    assertEquals(0, result.getAllSuperClasses().size());
    assertEquals(EntityWithLinkProperties.class.getSimpleName(), result.getName());
    assertEquals(EntityWithLinkProperties.class, result.getMaterializedEntity());

    var properties = result.propertiesMap(db);

    assertEquals(4, properties.size());
    assertEquals(PropertyType.LINK, properties.get("entityWithEmbeddedCollections").getType());

    var linkedType = properties.get("entityWithEmbeddedCollections").getLinkedClass();
    validateEntityWithEmbeddedCollections(linkedType);

    assertEquals(PropertyType.LINKSET,
        properties.get("entityWithPrimitivePropertiesSet").getType());
    linkedType = properties.get("entityWithPrimitivePropertiesSet").getLinkedClass();
    validateEntityWithSingleValueProperties(linkedType);

    assertEquals(PropertyType.LINKLIST, properties.get("emptyEntityList").getType());
    linkedType = properties.get("emptyEntityList").getLinkedClass();
    validateEmptyEntity(linkedType);

    assertEquals(PropertyType.LINKMAP,
        properties.get("entityWithEmbeddedCollectionsMap").getType());
    linkedType = properties.get("entityWithEmbeddedCollectionsMap").getLinkedClass();
    validateEntityWithEmbeddedCollections(linkedType);
  }

  @Test
  public void registerSelfReferencedEntity() {
    var schema = db.getSchema();
    var result = schema.registerMaterializedEntity(SelfReferencedEntity.class);

    validateSelfReferencedEntity(result);
  }

  private void validateSelfReferencedEntity(SchemaClass result) {
    assertEquals(0, result.getAllSuperClasses().size());
    assertEquals(SelfReferencedEntity.class.getSimpleName(), result.getName());
    assertEquals(SelfReferencedEntity.class, result.getMaterializedEntity());

    var properties = result.propertiesMap(db);

    assertEquals(1, properties.size());
    assertEquals(PropertyType.LINK, properties.get("selfReferencedEntity").getType());
    assertEquals(result, properties.get("selfReferencedEntity").getLinkedClass());
  }

  @Test
  public void registerMultiInheritanceEntity() {
    var schema = db.getSchema();
    var result = schema.registerMaterializedEntity(MultiInheritanceEntity.class);

    validateMultiInheritanceEntity(result);
  }

  private void validateMultiInheritanceEntity(SchemaClass result) {
    assertEquals(MultiInheritanceEntity.class.getSimpleName(), result.getName());
    assertEquals(MultiInheritanceEntity.class, result.getMaterializedEntity());

    HashMap<String, SchemaProperty> declaredProperties = new HashMap<>();
    result.declaredProperties().forEach(p -> declaredProperties.put(p.getName(), p));

    assertEquals(1, declaredProperties.size());
    assertEquals(PropertyType.LINKSET, declaredProperties.get("emptyEntitySet").getType());
    var linkedType = declaredProperties.get("emptyEntitySet").getLinkedClass();
    validateEmptyEntity(linkedType);

    Map<String, SchemaClass> superClasses = new HashMap<>();
    result.getAllSuperClasses().forEach(sc -> superClasses.put(sc.getName(), sc));

    assertEquals(2, superClasses.size());

    var entityWithLinkProperties = superClasses.get(EntityWithLinkProperties.class.getSimpleName());
    validateEntityWithLinkedProperties(entityWithLinkProperties);

    var entityWithEmbeddedCollections = superClasses.get(
        EntityWithEmbeddedCollections.class.getSimpleName());
    validateEntityWithEmbeddedCollections(entityWithEmbeddedCollections);
  }

  @Test
  public void testCyclicGraphProcessing() {
    var schema = db.getSchema();
    var result = schema.registerMaterializedEntity(CyclicGraphAEntity.class);

    validateCyclicGraphAEntity(result);
  }

  private void validateCyclicGraphAEntity(SchemaClass result) {
    assertEquals(CyclicGraphAEntity.class.getSimpleName(), result.getName());
    assertEquals(CyclicGraphAEntity.class, result.getMaterializedEntity());

    HashMap<String, SchemaProperty> declaredProperties = new HashMap<>();
    result.declaredProperties().forEach(p -> declaredProperties.put(p.getName(), p));

    assertEquals(2, declaredProperties.size());
    assertEquals(PropertyType.LINKLIST, declaredProperties.get("cyclicGraphListBEntity").getType());
    var linkedType = declaredProperties.get("cyclicGraphListBEntity").getLinkedClass();

    validateCyclicGraphBEntity(linkedType);

    assertEquals(PropertyType.LINK, declaredProperties.get("cyclicGraphCEntity").getType());
    linkedType = declaredProperties.get("cyclicGraphCEntity").getLinkedClass();
    validateCyclicGraphCEntity(linkedType);
  }

  private void validateCyclicGraphBEntity(SchemaClass result) {
    assertEquals(CyclicGraphBEntity.class.getSimpleName(), result.getName());
    assertEquals(CyclicGraphBEntity.class, result.getMaterializedEntity());

    HashMap<String, SchemaProperty> declaredProperties = new HashMap<>();
    result.declaredProperties().forEach(p -> declaredProperties.put(p.getName(), p));

    assertEquals(1, declaredProperties.size());
    assertEquals(PropertyType.LINKSET, declaredProperties.get("cyclicGraphSetCEntity").getType());

    var linkedType = declaredProperties.get("cyclicGraphSetCEntity").getLinkedClass();
    validateCyclicGraphCEntity(linkedType);
  }

  private void validateCyclicGraphCEntity(SchemaClass result) {
    assertEquals(CyclicGraphCEntity.class.getSimpleName(), result.getName());
    assertEquals(CyclicGraphCEntity.class, result.getMaterializedEntity());

    HashMap<String, SchemaProperty> declaredProperties = new HashMap<>();
    result.declaredProperties().forEach(p -> declaredProperties.put(p.getName(), p));

    assertEquals(1, declaredProperties.size());
    assertEquals(PropertyType.LINKMAP, declaredProperties.get("cyclicGraphMapAEntity").getType());

    var linkedType = declaredProperties.get("cyclicGraphMapAEntity").getLinkedClass();
    assertEquals(CyclicGraphAEntity.class.getSimpleName(), linkedType.getName());
  }
}
