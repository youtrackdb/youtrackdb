package com.jetbrains.youtrack.db.internal.common.monitoring.process;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.RecordSerialization")
@Description("Serialize record to stream")
@Label("Serialize record to stream")
@Category({"Process", "Serializer"})
@Enabled(false)
public class RecordSerializationEvent extends jdk.jfr.Event {

}

