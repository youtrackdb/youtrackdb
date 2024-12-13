package com.jetbrains.youtrack.db.auto;

import org.testng.annotations.Parameters;

public abstract class AbstractSelectTest extends BaseDBTest {

  @Parameters(value = "remote")
  protected AbstractSelectTest(boolean remote) {
    super(remote);
  }
}
