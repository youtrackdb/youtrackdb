package com.orientechnologies.orient.client.remote;

import com.orientechnologies.core.record.YTRecord;
import java.util.Set;

public interface OFetchPlanResults {

  Set<YTRecord> getFetchedRecordsToSend();
}
