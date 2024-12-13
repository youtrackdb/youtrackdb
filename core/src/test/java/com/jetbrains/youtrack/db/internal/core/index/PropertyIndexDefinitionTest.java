package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class PropertyIndexDefinitionTest extends DbTestBase {
  private PropertyIndexDefinition propertyIndex;

  @Before
  public void beforeMethod() {
    propertyIndex = new PropertyIndexDefinition("testClass", "fOne", PropertyType.INTEGER);
  }

  @Test
  public void testCreateValueSingleParameter() {
    final Object result = propertyIndex.createValue(db, Collections.singletonList("12"));
    Assert.assertEquals(12, result);
  }

  @Test
  public void testCreateValueTwoParameters() {
    final Object result = propertyIndex.createValue(db, Arrays.asList("12", "25"));
    Assert.assertEquals(12, result);
  }

  @Test(expected = DatabaseException.class)
  public void testCreateValueWrongParameter() {
    propertyIndex.createValue(db, Collections.singletonList("tt"));
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    final Object result = propertyIndex.createValue(db, "12");
    Assert.assertEquals(12, result);
  }

  @Test
  public void testCreateValueTwoParametersArrayParams() {
    final Object result = propertyIndex.createValue(db, "12", "25");
    Assert.assertEquals(12, result);
  }

  @Test(expected = DatabaseException.class)
  public void testCreateValueWrongParameterArrayParams() {
    propertyIndex.createValue(db, "tt");
  }

  @Test
  public void testGetDocumentValueToIndex() {
    final EntityImpl document = new EntityImpl();

    document.field("fOne", "15");
    document.field("fTwo", 10);

    final Object result = propertyIndex.getDocumentValueToIndex(db, document);
    Assert.assertEquals(15, result);
  }

  @Test
  public void testGetFields() {
    final List<String> result = propertyIndex.getFields();
    Assert.assertEquals(1, result.size());
    Assert.assertEquals("fOne", result.getFirst());
  }

  @Test
  public void testGetTypes() {
    final PropertyType[] result = propertyIndex.getTypes();
    Assert.assertEquals(1, result.length);
    Assert.assertEquals(PropertyType.INTEGER, result[0]);
  }

  @Test
  public void testEmptyIndexReload() {
    propertyIndex = new PropertyIndexDefinition("tesClass", "fOne", PropertyType.INTEGER);

    db.begin();
    final EntityImpl docToStore = propertyIndex.toStream(new EntityImpl());
    db.save(docToStore);
    db.commit();

    final EntityImpl docToLoad = db.load(docToStore.getIdentity());

    final PropertyIndexDefinition result = new PropertyIndexDefinition();
    result.fromStream(docToLoad);

    Assert.assertEquals(result, propertyIndex);
  }

  @Test
  public void testIndexReload() {
    final EntityImpl docToStore = propertyIndex.toStream(new EntityImpl());

    final PropertyIndexDefinition result = new PropertyIndexDefinition();
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
