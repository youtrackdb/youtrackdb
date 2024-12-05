package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import org.apache.commons.lang.RandomStringUtils;

/**
 *
 */
public class TestUtilsFixture extends DBTestBase {

  protected YTClass createClassInstance() {
    return getDBSchema().createClass(generateClassName());
  }

  protected YTClass createChildClassInstance(YTClass superclass) {
    return getDBSchema().createClass(generateClassName(), superclass);
  }

  private YTSchema getDBSchema() {
    return db.getMetadata().getSchema();
  }

  private static String generateClassName() {
    return "Class" + RandomStringUtils.randomNumeric(10);
  }
}
