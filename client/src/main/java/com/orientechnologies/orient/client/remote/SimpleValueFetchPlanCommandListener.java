package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.command.OCommandResultListener;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

public interface SimpleValueFetchPlanCommandListener extends OCommandResultListener {

  void linkdedBySimpleValue(EntityImpl doc);
}
