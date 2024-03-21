package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public abstract class AbstractSelectTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  protected AbstractSelectTest(@Optional String url) {
    super(url);
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db, Object... args) {
    return db.query(sql, args).stream()
        .map(OResult::asElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db, Map args) {
    return db.query(sql, args).stream()
        .map(OResult::asElement)
        .map(element -> (ODocument) element)
        .toList();
  }
}
