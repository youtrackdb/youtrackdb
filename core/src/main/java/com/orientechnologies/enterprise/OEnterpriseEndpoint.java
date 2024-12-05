package com.orientechnologies.enterprise;

import com.orientechnologies.core.db.YTDatabaseSession;

public interface OEnterpriseEndpoint {

  void haSetDbStatus(YTDatabaseSession db, String nodeName, String status);

  void haSetRole(YTDatabaseSession db, String nodeName, String role);

  void haSetOwner(YTDatabaseSession db, String clusterName, String owner);
}
