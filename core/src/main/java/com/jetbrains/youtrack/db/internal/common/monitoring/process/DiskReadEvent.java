package com.jetbrains.youtrack.db.internal.common.monitoring.process;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.DiskRead")
@Label("Disk read")
@Description("Amount of bytes read from the disk")
@Category({"Process", "Disk"})
@Enabled(false)
public class DiskReadEvent extends jdk.jfr.Event {

  private final long bytesRead;

  public DiskReadEvent(long bytesRead) {
    this.bytesRead = bytesRead;
  }
}
