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

package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.storage.index.sbtreebonsai.local.BonsaiBucketPointer;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.BonsaiCollectionPointer;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelDataOutput;
import java.io.IOException;

public class OCollectionNetworkSerializer {

  public static final OCollectionNetworkSerializer INSTANCE = new OCollectionNetworkSerializer();

  public OCollectionNetworkSerializer() {
  }

  public BonsaiCollectionPointer readCollectionPointer(ChannelDataInput client)
      throws IOException {
    final long fileId = client.readLong();
    final BonsaiBucketPointer rootPointer = readBonsaiBucketPointer(client);
    return new BonsaiCollectionPointer(fileId, rootPointer);
  }

  private BonsaiBucketPointer readBonsaiBucketPointer(ChannelDataInput client)
      throws IOException {
    long pageIndex = client.readLong();
    int pageOffset = client.readInt();
    return new BonsaiBucketPointer(pageIndex, pageOffset);
  }

  public void writeCollectionPointer(
      ChannelDataOutput client, BonsaiCollectionPointer treePointer) throws IOException {
    client.writeLong(treePointer.getFileId());
    client.writeLong(treePointer.getRootPointer().getPageIndex());
    client.writeInt(treePointer.getRootPointer().getPageOffset());
  }
}
