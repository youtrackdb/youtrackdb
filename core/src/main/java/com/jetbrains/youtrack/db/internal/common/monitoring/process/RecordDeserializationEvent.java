package com.jetbrains.youtrack.db.internal.common.monitoring.process;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.Deserialization")
@Description("Deserialize record from stream")
@Label("Deserialize record from stream")
@Category({"Process", "Serializer"})
@Enabled(false)
public class RecordDeserializationEvent extends jdk.jfr.Event {

}
