package com.jetbrains.youtrack.db.internal.common.monitoring.process;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Enabled;
import jdk.jfr.Label;
import jdk.jfr.Name;

@Name("com.jetbrains.youtrack.db.core.FieldSerialization")
@Description("Serialization to string")
@Label("Serialization to string")
@Category({"Process", "Serializer"})
@Enabled(false)
public class FieldSerializationEvent extends jdk.jfr.Event {

  private final String fieldType;

  public FieldSerializationEvent(PropertyType propertyType) {
    this.fieldType = propertyType.getName(); // jfr events don't support enums
  }

}

