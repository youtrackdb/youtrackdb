package com.orientechnologies.orient.core.enterprise;

import com.orientechnologies.orient.core.db.ODatabaseSession;

public interface OEnterpriseEndpoint {
  void haSetDbStatus(ODatabaseSession db, String nodeName, String status);

  void haSetRole(ODatabaseSession db, String nodeName, String role);

  void haSetOwner(ODatabaseSession db, String clusterName, String owner);
}
