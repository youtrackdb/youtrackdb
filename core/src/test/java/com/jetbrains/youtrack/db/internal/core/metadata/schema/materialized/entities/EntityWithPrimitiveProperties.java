package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.entities;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized.MaterializedEntity;

public interface EntityWithPrimitiveProperties extends MaterializedEntity {

  int getIntProperty();

  void setIntProperty(int value);

  long getLongProperty();

  void setLongProperty(long value);

  double getDoubleProperty();

  void setDoubleProperty(double value);

  float getFloatProperty();

  void setFloatProperty(float value);

  boolean getBooleanProperty();

  void setBooleanProperty(boolean value);

  byte getByteProperty();

  void setByteProperty(byte value);

  short getShortProperty();

  void setShortProperty(short value);
}
