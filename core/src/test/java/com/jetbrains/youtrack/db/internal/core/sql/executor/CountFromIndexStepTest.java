package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIndexIdentifier;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OIndexName;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class CountFromIndexStepTest extends TestUtilsFixture {

  private static final String PROPERTY_NAME = "testPropertyName";
  private static final String PROPERTY_VALUE = "testPropertyValue";
  private static final String ALIAS = "size";
  private String indexName;

  private final OIndexIdentifier.Type identifierType;

  public CountFromIndexStepTest(OIndexIdentifier.Type identifierType) {
    this.identifierType = identifierType;
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> types() {
    return Arrays.asList(
        new Object[][]{
            {OIndexIdentifier.Type.INDEX},
            {OIndexIdentifier.Type.VALUES},
            {OIndexIdentifier.Type.VALUESASC},
            {OIndexIdentifier.Type.VALUESDESC},
        });
  }

  public void beforeTest() throws Exception {
    super.beforeTest();
    YTClass clazz = createClassInstance();
    clazz.createProperty(db, PROPERTY_NAME, YTType.STRING);
    String className = clazz.getName();
    indexName = className + "." + PROPERTY_NAME;
    clazz.createIndex(db, indexName, YTClass.INDEX_TYPE.NOTUNIQUE, PROPERTY_NAME);

    for (int i = 0; i < 20; i++) {
      db.begin();
      EntityImpl document = new EntityImpl(className);
      document.field(PROPERTY_NAME, PROPERTY_VALUE);
      document.save();
      db.commit();
    }
  }

  @Test
  public void shouldCountRecordsOfIndex() {
    OIndexName name = new OIndexName(-1);
    name.setValue(indexName);
    OIndexIdentifier identifier = new OIndexIdentifier(-1);
    identifier.setIndexName(name);
    identifier.setIndexNameString(name.getValue());
    identifier.setType(identifierType);

    BasicCommandContext context = new BasicCommandContext();
    context.setDatabase(db);
    CountFromIndexStep step = new CountFromIndexStep(identifier, ALIAS, context, false);

    ExecutionStream result = step.start(context);
    Assert.assertEquals(20, (long) result.next(context).getProperty(ALIAS));
    Assert.assertFalse(result.hasNext(context));
  }
}
