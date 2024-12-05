package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.core.YouTrackDBManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class AbstractIndexReuseTest extends DocumentDBBaseTest {

  protected OProfiler profiler;

  public AbstractIndexReuseTest(boolean remote) {
    super(remote);
  }

  @Override
  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    profiler = getProfilerInstance();
    if (!profiler.isRecording()) {
      profiler.startRecording();
    }
  }

  @AfterClass
  @Override
  public void afterClass() throws Exception {
    super.afterClass();
  }

  private OProfiler getProfilerInstance() throws Exception {
    return YouTrackDBManager.instance().getProfiler();
  }
}
