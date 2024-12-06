package com.jetbrains.youtrack.db.internal.common.profiler;

import com.jetbrains.youtrack.db.internal.common.profiler.AbstractProfiler.ProfilerHookValue;
import com.jetbrains.youtrack.db.internal.common.types.ModifiableLong;

public class ModifiableLongProfileHookValue implements ProfilerHookValue {

  private final ModifiableLong value;

  public ModifiableLongProfileHookValue(ModifiableLong value) {
    this.value = value;
  }

  @Override
  public Object getValue() {
    return value.getValue();
  }
}
