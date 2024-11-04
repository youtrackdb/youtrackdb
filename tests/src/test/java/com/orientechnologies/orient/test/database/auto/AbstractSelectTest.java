package com.orientechnologies.orient.test.database.auto;

import org.testng.annotations.Parameters;

public abstract class AbstractSelectTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  protected AbstractSelectTest(boolean remote) {
    super(remote);
  }
}
