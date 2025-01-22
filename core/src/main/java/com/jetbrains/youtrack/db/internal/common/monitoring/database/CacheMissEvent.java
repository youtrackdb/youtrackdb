package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.CacheMiss")
@Description("Record not found in Level1 Cache")
@Label("Cache Miss")
@Category({"Database", "Cache"})
@Enabled(false)
public class CacheMissEvent extends jdk.jfr.Event {
}
