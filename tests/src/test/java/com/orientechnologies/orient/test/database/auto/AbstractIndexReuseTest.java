package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.common.profiler.Profiler;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class AbstractIndexReuseTest extends DocumentDBBaseTest {

  protected Profiler profiler;

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

  private Profiler getProfilerInstance() throws Exception {
    return YouTrackDBEnginesManager.instance().getProfiler();
  }
}
