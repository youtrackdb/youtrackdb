package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 7/3/14
 */
@Test
public abstract class DocumentDBBaseTest extends BaseTest<ODatabaseDocumentInternal> {
  protected DocumentDBBaseTest() {}

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url) {
    super(url);
  }

  @Parameters(value = "url")
  protected DocumentDBBaseTest(@Optional String url, String prefix) {
    super(url, prefix);
  }

  protected ODatabaseDocumentInternal createDatabaseInstance(String url) {
    return new ODatabaseDocumentTx(url);
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db, Object... args) {
    return db.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db, Map args) {
    return db.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, ODatabaseDocumentInternal db) {
    return db.query(sql).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, Object... args) {
    return database.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql, Map args) {
    return database.query(sql, args).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }

  protected List<ODocument> executeQuery(String sql) {
    return database.query(sql).stream()
        .map(OResult::toElement)
        .map(element -> (ODocument) element)
        .toList();
  }
}
