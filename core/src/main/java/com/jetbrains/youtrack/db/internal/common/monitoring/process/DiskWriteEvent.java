package com.jetbrains.youtrack.db.internal.common.monitoring.process;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DiskWrite")
@Label("Disk write")
@Description("Amount of bytes written to the disk")
@Category({"Process", "Disk"})
@Enabled(false)
public class DiskWriteEvent extends jdk.jfr.Event {

  private final long bytesWritten;

  public DiskWriteEvent(long bytesWritten) {
    this.bytesWritten = bytesWritten;
  }
}
