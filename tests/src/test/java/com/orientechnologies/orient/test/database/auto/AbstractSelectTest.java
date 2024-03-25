package com.orientechnologies.orient.test.database.auto;

import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

public abstract class AbstractSelectTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  protected AbstractSelectTest(@Optional String url) {
    super(url);
  }
}
