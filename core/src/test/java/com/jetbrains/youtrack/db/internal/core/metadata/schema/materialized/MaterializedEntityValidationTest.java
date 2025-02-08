package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Test;

public class MaterializedEntityValidationTest extends DbTestBase {
  @Test
  public void registerEmptyEntity() {
      var schema = db.getSchema();
      interface EmptyEntity extends MaterializedEntity {
      }

      var result = schema.registerMaterializedEntity(EmptyEntity.class);

      assertEquals(0, result.properties(db).size());
      assertEquals(0, result.getAllSuperClasses().size());
      assertEquals(EmptyEntity.class.getSimpleName(), result.getName());
      assertEquals(EmptyEntity.class, result.getMaterializedEntity());
  }
}
