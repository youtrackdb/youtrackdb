/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import java.util.Locale;

/**
 * Contains the information about a database operation.
 */
public final class ORecordOperation implements Comparable<ORecordOperation> {

  public static final byte UPDATED = 1;
  public static final byte DELETED = 2;
  public static final byte CREATED = 3;

  public byte type;
  public YTRecordAbstract record;
  // used in processing of server transactions
  public boolean callHooksOnServerTx = false;

  public ORecordOperation() {
  }

  public ORecordOperation(final YTRecordAbstract record, final byte status) {
    // CLONE RECORD AND CONTENT
    this.record = record;
    this.type = status;
  }

  @Override
  public int hashCode() {
    return record.getIdentity().hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (!(obj instanceof ORecordOperation)) {
      return false;
    }

    return record.equals(((ORecordOperation) obj).record);
  }

  @Override
  public String toString() {
    return "ORecordOperation [record=" + record + ", type=" + getName(type) + "]";
  }

  public YTRID getRID() {
    return record != null ? record.getIdentity() : null;
  }

  public static String getName(final int type) {
    return switch (type) {
      case ORecordOperation.CREATED -> "CREATE";
      case ORecordOperation.UPDATED -> "UPDATE";
      case ORecordOperation.DELETED -> "DELETE";
      default -> "?";
    };
  }

  public static byte getId(String iName) {
    iName = iName.toUpperCase(Locale.ENGLISH);

    if (iName.startsWith("CREAT")) {
      return ORecordOperation.CREATED;
    } else if (iName.startsWith("UPDAT")) {
      return ORecordOperation.UPDATED;
    } else if (iName.startsWith("DELET")) {
      return ORecordOperation.DELETED;
    } else {
      return -1;
    }
  }

  @Override
  public int compareTo(ORecordOperation o) {
    return record.compareTo(o.record);
  }
}
