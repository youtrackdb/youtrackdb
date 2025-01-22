package com.jetbrains.youtrack.db.internal.common.monitoring.database;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.CacheHit")
@Description("Record found in Level1 Cache")
@Label("Cache Hit")
@Category({"Database", "Cache"})
@Enabled(false)
public class CacheHitEvent extends jdk.jfr.Event {
}
