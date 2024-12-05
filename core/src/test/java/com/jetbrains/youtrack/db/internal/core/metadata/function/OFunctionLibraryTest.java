package com.jetbrains.youtrack.db.internal.core.metadata.function;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import org.junit.Test;

/**
 *
 */
public class OFunctionLibraryTest extends DBTestBase {

  @Test
  public void testSimpleFunctionCreate() {
    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNotNull(func);
  }

  @Test(expected = YTFunctionDuplicatedException.class)
  public void testDuplicateFunctionCreate() {
    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
  }

  @Test
  public void testFunctionCreateDrop() {
    OFunction func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNotNull(func);
    db.getMetadata().getFunctionLibrary().dropFunction(db, "TestFunc");
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNull(func);
    func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc1");
    db.begin();
    db.getMetadata().getFunctionLibrary().dropFunction(db, func);
    db.commit();
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNull(func);
  }
}
