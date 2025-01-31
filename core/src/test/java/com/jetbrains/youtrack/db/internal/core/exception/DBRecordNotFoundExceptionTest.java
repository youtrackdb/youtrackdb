package com.jetbrains.youtrack.db.internal.core.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import org.junit.Test;

/**
 *
 */
public class DBRecordNotFoundExceptionTest {

  @Test
  public void simpleExceptionCopyTest() {
    var ex = new RecordNotFoundException(new RecordId(1, 2));
    var ex1 = new RecordNotFoundException(ex);
    assertNotNull(ex1.getRid());
    assertEquals(ex1.getRid().getClusterId(), 1);
    assertEquals(ex1.getRid().getClusterPosition(), 2);
  }
}
