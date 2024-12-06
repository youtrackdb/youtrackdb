package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
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
    Assert.assertEquals(result, 12);
  }

  @Test
  public void testCreateValueTwoParameters() {
    final Object result = propertyIndex.createValue(db, Arrays.asList("12", "25"));
    Assert.assertEquals(result, 12);
  }

  @Test(expected = DatabaseException.class)
  public void testCreateValueWrongParameter() {
    propertyIndex.createValue(db, Collections.singletonList("tt"));
  }

  @Test
  public void testCreateValueSingleParameterArrayParams() {
    final Object result = propertyIndex.createValue(db, "12");
    Assert.assertEquals(result, 12);
  }

  @Test
  public void testCreateValueTwoParametersArrayParams() {
    final Object result = propertyIndex.createValue(db, "12", "25");
    Assert.assertEquals(result, 12);
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
    Assert.assertEquals(result, 15);
  }

  @Test
  public void testGetFields() {
    final List<String> result = propertyIndex.getFields();
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0), "fOne");
  }

  @Test
  public void testGetTypes() {
    final PropertyType[] result = propertyIndex.getTypes();
    Assert.assertEquals(result.length, 1);
    Assert.assertEquals(result[0], PropertyType.INTEGER);
  }

  @Test
  public void testEmptyIndexReload() {
    final DatabaseDocumentTx database = new DatabaseDocumentTx("memory:propertytest");
    database.create();

    propertyIndex = new PropertyIndexDefinition("tesClass", "fOne", PropertyType.INTEGER);

    database.begin();
    final EntityImpl docToStore = propertyIndex.toStream(new EntityImpl());
    database.save(docToStore, database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final EntityImpl docToLoad = database.load(docToStore.getIdentity());

    final PropertyIndexDefinition result = new PropertyIndexDefinition();
    result.fromStream(docToLoad);

    database.drop();
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
    Assert.assertEquals(propertyIndex.getParamCount(), 1);
  }

  @Test
  public void testClassName() {
    Assert.assertEquals("testClass", propertyIndex.getClassName());
  }
}
