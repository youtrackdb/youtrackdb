package com.orientechnologies.orient.core.id;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import javax.annotation.Nonnull;

public class ChangeableRecordId extends ORecordId implements ChangeableIdentity {

  private Set<IdentityChangeListener> identityChangeListeners;

  /**
   * Counter for temporal identity of record id till it will not be defined during storage of
   * record. This counter is not thread safe because we care only about difference in single thread.
   * Even if number will be duplicated in different threads it does not matter for us. JVM
   * guarantees atomicity arithmetic operations on <code>int</code>s
   */
  private static int tempIdCounter = 0;

  /**
   * Temporary identity of record id. It is used to identify record id in memory before it will be
   * stored in database and will get real identity. If record id would not be comparable we would
   * not need it.
   */
  private int tempId;

  public ChangeableRecordId() {
    tempId = tempIdCounter++;
  }

  public void setClusterId(int clusterId) {
    if (clusterId == this.clusterId) {
      return;
    }

    checkClusterLimits(clusterId);

    fireBeforeIdentityChange();
    this.clusterId = clusterId;
    fireAfterIdentityChange();
  }

  public void setClusterPosition(long clusterPosition) {
    if (clusterPosition == this.clusterPosition) {
      return;
    }

    fireBeforeIdentityChange();
    this.clusterPosition = clusterPosition;
    fireAfterIdentityChange();
  }

  public void addIdentityChangeListener(IdentityChangeListener identityChangeListeners) {
    if (!canChangeIdentity()) {
      return;
    }

    if (this.identityChangeListeners == null) {
      this.identityChangeListeners = Collections.newSetFromMap(new WeakHashMap<>());
    }

    this.identityChangeListeners.add(identityChangeListeners);
  }

  public void removeIdentityChangeListener(IdentityChangeListener identityChangeListener) {
    if (this.identityChangeListeners != null) {
      this.identityChangeListeners.remove(identityChangeListener);

      if (this.identityChangeListeners.isEmpty()) {
        this.identityChangeListeners = null;
      }
    }
  }

  private void fireBeforeIdentityChange() {
    if (this.identityChangeListeners != null) {
      for (IdentityChangeListener listener : this.identityChangeListeners) {
        listener.onBeforeIdentityChange(this);
      }
    }
  }

  private void fireAfterIdentityChange() {
    if (this.identityChangeListeners != null) {
      for (IdentityChangeListener listener : this.identityChangeListeners) {
        listener.onAfterIdentityChange(this);
      }
    }
  }

  @Override
  public boolean canChangeIdentity() {
    return !isValid();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (!(obj instanceof OIdentifiable)) {
      return false;
    }
    final ORecordId other = (ORecordId) ((OIdentifiable) obj).getIdentity();

    if (clusterId == other.clusterId && clusterPosition == other.clusterPosition) {
      if (clusterId != CLUSTER_ID_INVALID || clusterPosition != CLUSTER_POS_INVALID) {
        return true;
      }

      if (other instanceof ChangeableRecordId otherRecordId) {
        return tempId == otherRecordId.tempId;
      }

      return true;
    }

    return false;
  }

  @Override
  public int hashCode() {
    if (clusterPosition != CLUSTER_POS_INVALID || clusterId != CLUSTER_ID_INVALID) {
      return 31 * clusterId + 103 * (int) clusterPosition;
    }

    return 31 * clusterId + 103 * (int) clusterPosition + 17 * tempId;
  }

  public int compareTo(@Nonnull final OIdentifiable other) {
    if (other == this) {
      return 0;
    }

    var otherIdentity = other.getIdentity();
    final int otherClusterId = otherIdentity.getClusterId();
    if (clusterId == otherClusterId) {
      final long otherClusterPos = other.getIdentity().getClusterPosition();

      if (clusterPosition == otherClusterPos) {
        if ((clusterId == CLUSTER_ID_INVALID && clusterPosition == CLUSTER_POS_INVALID)
            && otherIdentity instanceof ChangeableRecordId otherRecordId) {
          return Integer.compare(tempId, otherRecordId.tempId);
        }

        return 0;
      }

      return Long.compare(clusterPosition, otherClusterPos);
    } else if (clusterId > otherClusterId) {
      return 1;
    }

    return -1;
  }

  public ORecordId copy() {
    if (clusterId == CLUSTER_ID_INVALID && clusterPosition == CLUSTER_POS_INVALID) {
      var recordId = new ChangeableRecordId();
      recordId.clusterId = clusterId;
      recordId.clusterPosition = clusterPosition;
      recordId.tempId = tempId;

      return recordId;
    }

    var recordId = new ORecordId();
    recordId.clusterId = clusterId;
    recordId.clusterPosition = clusterPosition;

    return recordId;
  }
}
