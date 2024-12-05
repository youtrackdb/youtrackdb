package com.orientechnologies.orient.client.remote;

import com.orientechnologies.core.command.OCommandResultListener;
import com.orientechnologies.core.record.impl.YTEntityImpl;

public interface SimpleValueFetchPlanCommandListener extends OCommandResultListener {

  void linkdedBySimpleValue(YTEntityImpl doc);
}
