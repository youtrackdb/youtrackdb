package com.jetbrains.youtrack.db.internal.core.metadata.function;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Test;

/**
 *
 */
public class FunctionLibraryTest extends DbTestBase {

  @Test
  public void testSimpleFunctionCreate() {
    var func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    func = db.getMetadata().getFunctionLibrary().getFunction("TestFunc");
    assertNotNull(func);
  }

  @Test(expected = FunctionDuplicatedException.class)
  public void testDuplicateFunctionCreate() {
    var func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
    assertNotNull(func);
    db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
  }

  @Test
  public void testFunctionCreateDrop() {
    var func = db.getMetadata().getFunctionLibrary().createFunction("TestFunc");
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
