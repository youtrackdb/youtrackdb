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

public class SimpleKeyIndexDefinitionTest extends DbTestBase {

  private SimpleKeyIndexDefinition simpleKeyIndexDefinition;

  @Before
  public void beforeMethod() {
    simpleKeyIndexDefinition = new SimpleKeyIndexDefinition(PropertyType.INTEGER,
        PropertyType.STRING);
  }

  @Test
  public void testGetFields() {
    Assert.assertTrue(simpleKeyIndexDefinition.getFields().isEmpty());
  }

  @Test
  public void testGetClassName() {
    Assert.assertNull(simpleKeyIndexDefinition.getClassName());
  }

  @Test
  public void testCreateValueSimpleKey() {
    final var keyIndexDefinition =
        new SimpleKeyIndexDefinition(PropertyType.INTEGER);
    final var result = keyIndexDefinition.createValue(session, "2");
    Assert.assertEquals(2, result);
  }

  @Test
  public void testCreateValueCompositeKeyListParam() {
    final var result = simpleKeyIndexDefinition.createValue(session, Arrays.asList("2", "3"));

    final var compositeKey = new CompositeKey(Arrays.asList(2, "3"));
    Assert.assertEquals(result, compositeKey);
  }

  @Test
  public void testCreateValueCompositeKeyNullListParam() {
    final var result =
        simpleKeyIndexDefinition.createValue(session, Collections.singletonList(null));

    Assert.assertNull(result);
  }

  @Test
  public void testNullParamListItem() {
    final var result = simpleKeyIndexDefinition.createValue(session, Arrays.asList("2", null));

    Assert.assertNull(result);
  }

  @Test(expected = DatabaseException.class)
  public void testWrongParamTypeListItem() {
    simpleKeyIndexDefinition.createValue(session, Arrays.asList("a", "3"));
  }

  @Test
  public void testCreateValueCompositeKey() {
    final var result = simpleKeyIndexDefinition.createValue(session, "2", "3");

    final var compositeKey = new CompositeKey(Arrays.asList(2, "3"));
    Assert.assertEquals(result, compositeKey);
  }

  @Test
  public void testCreateValueCompositeKeyNullParamList() {
    final var result = simpleKeyIndexDefinition.createValue(session, (List<?>) null);

    Assert.assertNull(result);
  }

  @Test
  public void testCreateValueCompositeKeyNullParam() {
    final var result = simpleKeyIndexDefinition.createValue(session, (Object) null);

    Assert.assertNull(result);
  }

  @Test
  public void testCreateValueCompositeKeyEmptyList() {
    final var result = simpleKeyIndexDefinition.createValue(session, Collections.emptyList());

    Assert.assertNull(result);
  }

  @Test
  public void testNullParamItem() {
    final var result = simpleKeyIndexDefinition.createValue(session, "2", null);

    Assert.assertNull(result);
  }

  @Test(expected = DatabaseException.class)
  public void testWrongParamType() {
    simpleKeyIndexDefinition.createValue(session, "a", "3");
  }

  @Test
  public void testParamCount() {
    Assert.assertEquals(2, simpleKeyIndexDefinition.getParamCount());
  }

  @Test
  public void testParamCountOneItem() {
    final var keyIndexDefinition =
        new SimpleKeyIndexDefinition(PropertyType.INTEGER);

    Assert.assertEquals(1, keyIndexDefinition.getParamCount());
  }

  @Test
  public void testGetKeyTypes() {
    Assert.assertEquals(
        new PropertyType[]{PropertyType.INTEGER, PropertyType.STRING},
        simpleKeyIndexDefinition.getTypes());
  }

  @Test
  public void testGetKeyTypesOneType() {
    final var keyIndexDefinition =
        new SimpleKeyIndexDefinition(PropertyType.BOOLEAN);

    Assert.assertEquals(new PropertyType[]{PropertyType.BOOLEAN}, keyIndexDefinition.getTypes());
  }

  @Test
  public void testReload() {
    final var map = simpleKeyIndexDefinition.toMap(session);
    final var loadedKeyIndexDefinition = new SimpleKeyIndexDefinition();

    loadedKeyIndexDefinition.fromMap(map);
    Assert.assertEquals(loadedKeyIndexDefinition, simpleKeyIndexDefinition);
  }

  @Test(expected = IndexException.class)
  public void testGetDocumentValueToIndex() {
    session.begin();
    simpleKeyIndexDefinition.getDocumentValueToIndex(session, (EntityImpl) session.newEntity());
    session.rollback();
  }
}
