package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SchemaPropertyIndexDefinitionTest extends DbTestBase {
  private PropertyIndexDefinition propertyIndex;

  @Before
  public void beforeMethod() {
    propertyIndex = new PropertyIndexDefinition("testClass", "fOne", PropertyType.INTEGER);
  }

  @Test
  public void testCreateValueSingleParameter() {
    final var result = propertyIndex.createValue(db, Collections.singletonList("12"));
    Assert.assertEquals(12, result);
  }

  @Test
  public void testCreateValueTwoParameters() {
    final var result = propertyIndex.createValue(db, Arrays.asList("12", "25"));
    Assert.assertEquals(12, result);
  }

  @Test(expected = DatabaseException.class)
  public void testCreateValueWrongParameter() {
    propertyIndex.createValue(db, Collections.singletonList("tt"));
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    final var result = propertyIndex.createValue(db, "12");
    Assert.assertEquals(12, result);
  }

  @Test
  public void testCreateValueTwoParametersArrayParams() {
    final var result = propertyIndex.createValue(db, "12", "25");
    Assert.assertEquals(12, result);
  }

  @Test(expected = DatabaseException.class)
  public void testCreateValueWrongParameterArrayParams() {
    propertyIndex.createValue(db, "tt");
  }

  @Test
  public void testGetDocumentValueToIndex() {
    final var document = (EntityImpl) db.newEntity();

    document.field("fOne", "15");
    document.field("fTwo", 10);

    final var result = propertyIndex.getDocumentValueToIndex(db, document);
    Assert.assertEquals(15, result);
  }

  @Test
  public void testGetFields() {
    final var result = propertyIndex.getFields();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("fOne", result.getFirst());
  }

  @Test
  public void testGetTypes() {
    final var result = propertyIndex.getTypes();
    Assert.assertEquals(1, result.length);
    Assert.assertEquals(PropertyType.INTEGER, result[0]);
  }

  @Test
  public void testEmptyIndexReload() {
    propertyIndex = new PropertyIndexDefinition("tesClass", "fOne", PropertyType.INTEGER);

    db.begin();
    final var docToStore = propertyIndex.toStream(db, (EntityImpl) db.newEntity());
    db.save(docToStore);
    db.commit();

    final EntityImpl docToLoad = db.load(docToStore.getIdentity());

    final var result = new PropertyIndexDefinition();
    result.fromStream(docToLoad);

    Assert.assertEquals(result, propertyIndex);
  }

  @Test
  public void testIndexReload() {
    final var docToStore = propertyIndex.toStream(db, (EntityImpl) db.newEntity());

    final var result = new PropertyIndexDefinition();
    result.fromStream(docToStore);

    Assert.assertEquals(result, propertyIndex);
  }

  @Test
  public void testGetParamCount() {
    Assert.assertEquals(1, propertyIndex.getParamCount());
  }

  @Test
  public void testClassName() {
    Assert.assertEquals("testClass", propertyIndex.getClassName());
  }
}
