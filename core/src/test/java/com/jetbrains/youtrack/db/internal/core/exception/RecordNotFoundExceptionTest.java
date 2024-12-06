package com.jetbrains.youtrack.db.internal.core.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import org.junit.Test;

/**
 *
 */
public class RecordNotFoundExceptionTest {

  @Test
  public void simpleExceptionCopyTest() {
    RecordNotFoundException ex = new RecordNotFoundException(new RecordId(1, 2));
    RecordNotFoundException ex1 = new RecordNotFoundException(ex);
    assertNotNull(ex1.getRid());
    assertEquals(ex1.getRid().getClusterId(), 1);
    assertEquals(ex1.getRid().getClusterPosition(), 2);
  }
}
