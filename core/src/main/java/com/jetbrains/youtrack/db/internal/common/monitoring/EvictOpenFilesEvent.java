package com.jetbrains.youtrack.db.internal.common.monitoring;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.EvictOpenFiles")
@Description("Close the opened files because reached the configured limit")
@Label("Evict open files")
@Category("Database")
@Enabled(false)
public class EvictOpenFilesEvent extends jdk.jfr.Event {

}
