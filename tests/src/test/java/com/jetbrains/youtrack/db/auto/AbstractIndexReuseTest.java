package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.internal.common.profiler.Profiler;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

public abstract class AbstractIndexReuseTest extends BaseDBTest {

  protected Profiler profiler;
  protected IndexTestingHelper tester;

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

  @BeforeMethod
  public void beforeMethod() throws Exception {
    super.beforeMethod();
    this.tester = new IndexTestingHelper(database, false);
  }

  private Profiler getProfilerInstance() throws Exception {
    return YouTrackDBEnginesManager.instance().getProfiler();
  }
}
