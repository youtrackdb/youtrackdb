/*
 *
 *  *  Copyright YouTrackDB
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
package com.orientechnologies.orient.server.network.protocol.binary;

import com.orientechnologies.orient.client.remote.OBinaryRequest;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.message.OAddClusterRequest;
import com.orientechnologies.orient.client.remote.message.OBeginTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OBeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OCeilingPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OCleanOutRecordRequest;
import com.orientechnologies.orient.client.remote.message.OCloseQueryRequest;
import com.orientechnologies.orient.client.remote.message.OCloseRequest;
import com.orientechnologies.orient.client.remote.message.OCommandRequest;
import com.orientechnologies.orient.client.remote.message.OCommit37Request;
import com.orientechnologies.orient.client.remote.message.OCommit38Request;
import com.orientechnologies.orient.client.remote.message.OCommitRequest;
import com.orientechnologies.orient.client.remote.message.OConnect37Request;
import com.orientechnologies.orient.client.remote.message.OConnectRequest;
import com.orientechnologies.orient.client.remote.message.OCountRecordsRequest;
import com.orientechnologies.orient.client.remote.message.OCountRequest;
import com.orientechnologies.orient.client.remote.message.OCreateDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OCreateRecordRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedConnectRequest;
import com.orientechnologies.orient.client.remote.message.ODistributedStatusRequest;
import com.orientechnologies.orient.client.remote.message.ODropClusterRequest;
import com.orientechnologies.orient.client.remote.message.ODropDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OExistsDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OFetchTransaction38Request;
import com.orientechnologies.orient.client.remote.message.OFetchTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OFloorPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OFreezeDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OGetClusterDataRangeRequest;
import com.orientechnologies.orient.client.remote.message.OGetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OGetRecordMetadataRequest;
import com.orientechnologies.orient.client.remote.message.OGetSizeRequest;
import com.orientechnologies.orient.client.remote.message.OHigherPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OImportRequest;
import com.orientechnologies.orient.client.remote.message.OIncrementalBackupRequest;
import com.orientechnologies.orient.client.remote.message.OListDatabasesRequest;
import com.orientechnologies.orient.client.remote.message.OListGlobalConfigurationsRequest;
import com.orientechnologies.orient.client.remote.message.OLowerPhysicalPositionsRequest;
import com.orientechnologies.orient.client.remote.message.OOpen37Request;
import com.orientechnologies.orient.client.remote.message.OOpenRequest;
import com.orientechnologies.orient.client.remote.message.OQueryNextPageRequest;
import com.orientechnologies.orient.client.remote.message.OQueryRequest;
import com.orientechnologies.orient.client.remote.message.OReadRecordRequest;
import com.orientechnologies.orient.client.remote.message.ORebeginTransaction38Request;
import com.orientechnologies.orient.client.remote.message.ORebeginTransactionRequest;
import com.orientechnologies.orient.client.remote.message.ORecordExistsRequest;
import com.orientechnologies.orient.client.remote.message.OReleaseDatabaseRequest;
import com.orientechnologies.orient.client.remote.message.OReloadRequest;
import com.orientechnologies.orient.client.remote.message.OReloadRequest37;
import com.orientechnologies.orient.client.remote.message.OReopenRequest;
import com.orientechnologies.orient.client.remote.message.ORollbackTransactionRequest;
import com.orientechnologies.orient.client.remote.message.OSBTCreateTreeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFetchEntriesMajorRequest;
import com.orientechnologies.orient.client.remote.message.OSBTFirstKeyRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRealBagSizeRequest;
import com.orientechnologies.orient.client.remote.message.OSBTGetRequest;
import com.orientechnologies.orient.client.remote.message.OSendTransactionStateRequest;
import com.orientechnologies.orient.client.remote.message.OServerInfoRequest;
import com.orientechnologies.orient.client.remote.message.OServerQueryRequest;
import com.orientechnologies.orient.client.remote.message.OSetGlobalConfigurationRequest;
import com.orientechnologies.orient.client.remote.message.OShutdownRequest;
import com.orientechnologies.orient.client.remote.message.OSubscribeRequest;
import com.orientechnologies.orient.client.remote.message.OUnsubscribeRequest;
import com.orientechnologies.orient.client.remote.message.OUpdateRecordRequest;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import java.util.function.Function;

/**
 *
 */
public class ONetworkBinaryProtocolFactory {

  private static final Function<Integer, OBinaryRequest<? extends OBinaryResponse>>
      defaultProtocol = ONetworkBinaryProtocolFactory::createRequest;

  public static Function<Integer, OBinaryRequest<? extends OBinaryResponse>> defaultProtocol() {
    return defaultProtocol;
  }

  public static Function<Integer, OBinaryRequest<? extends OBinaryResponse>> matchProtocol(
      short protocolVersion) {
    return switch (protocolVersion) {
      case 37 -> ONetworkBinaryProtocolFactory::createRequest37;
      case 38 -> ONetworkBinaryProtocolFactory::createRequest38;
      default -> ONetworkBinaryProtocolFactory::createRequest;
    };
  }

  /**
   * Legacy Protocol < 37
   */
  private static OBinaryRequest<? extends OBinaryResponse> createRequest(int requestType) {
    return switch (requestType) {
      case ChannelBinaryProtocol.REQUEST_DB_OPEN -> new OOpenRequest();
      case ChannelBinaryProtocol.REQUEST_CONNECT -> new OConnectRequest();
      case ChannelBinaryProtocol.REQUEST_DB_REOPEN -> new OReopenRequest();
      case ChannelBinaryProtocol.REQUEST_SHUTDOWN -> new OShutdownRequest();
      case ChannelBinaryProtocol.REQUEST_DB_LIST -> new OListDatabasesRequest();
      case ChannelBinaryProtocol.REQUEST_SERVER_INFO -> new OServerInfoRequest();
      case ChannelBinaryProtocol.REQUEST_DB_RELOAD -> new OReloadRequest();
      case ChannelBinaryProtocol.REQUEST_DB_CREATE -> new OCreateDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_CLOSE -> new OCloseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_EXIST -> new OExistsDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_DROP -> new ODropDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_SIZE -> new OGetSizeRequest();
      case ChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS -> new OCountRecordsRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER -> new ODistributedStatusRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_COUNT -> new OCountRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE -> new OGetClusterDataRangeRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_ADD -> new OAddClusterRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_DROP -> new ODropClusterRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_METADATA -> new OGetRecordMetadataRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_LOAD -> new OReadRecordRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_EXISTS -> new ORecordExistsRequest();
      case ChannelBinaryProtocol.REQUEST_SEND_TRANSACTION_STATE ->
          new OSendTransactionStateRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_CREATE -> new OCreateRecordRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_UPDATE -> new OUpdateRecordRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER -> new OHigherPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_CEILING ->
          new OCeilingPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_LOWER -> new OLowerPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR -> new OFloorPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_COMMAND -> new OCommandRequest();
      case ChannelBinaryProtocol.REQUEST_SERVER_QUERY -> new OServerQueryRequest();
      case ChannelBinaryProtocol.REQUEST_QUERY -> new OQueryRequest();
      case ChannelBinaryProtocol.REQUEST_CLOSE_QUERY -> new OCloseQueryRequest();
      case ChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE -> new OQueryNextPageRequest();
      case ChannelBinaryProtocol.REQUEST_TX_COMMIT -> new OCommitRequest();
      case ChannelBinaryProtocol.REQUEST_CONFIG_GET -> new OGetGlobalConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_CONFIG_SET -> new OSetGlobalConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_CONFIG_LIST -> new OListGlobalConfigurationsRequest();
      case ChannelBinaryProtocol.REQUEST_DB_FREEZE -> new OFreezeDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_RELEASE -> new OReleaseDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT -> new OCleanOutRecordRequest();
      case ChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI -> new OSBTCreateTreeRequest();
      case ChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET -> new OSBTGetRequest();
      case ChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY -> new OSBTFirstKeyRequest();
      case ChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR ->
          new OSBTFetchEntriesMajorRequest<>();
      case ChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE -> new OSBTGetRealBagSizeRequest();
      case ChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP -> new OIncrementalBackupRequest();
      case ChannelBinaryProtocol.REQUEST_DB_IMPORT -> new OImportRequest();
      case ChannelBinaryProtocol.DISTRIBUTED_CONNECT -> new ODistributedConnectRequest();
      default -> throw new DatabaseException("binary protocol command with code: " + requestType);
    };
  }

  /**
   * Protocol 37
   */
  public static OBinaryRequest<? extends OBinaryResponse> createRequest37(int requestType) {
    return switch (requestType) {
      case ChannelBinaryProtocol.SUBSCRIBE_PUSH -> new OSubscribeRequest();
      case ChannelBinaryProtocol.UNSUBSCRIBE_PUSH -> new OUnsubscribeRequest();
      case ChannelBinaryProtocol.REQUEST_TX_FETCH -> new OFetchTransactionRequest();
      case ChannelBinaryProtocol.REQUEST_TX_REBEGIN -> new ORebeginTransactionRequest();
      case ChannelBinaryProtocol.REQUEST_TX_BEGIN -> new OBeginTransactionRequest();
      case ChannelBinaryProtocol.REQUEST_TX_COMMIT -> new OCommit37Request();
      case ChannelBinaryProtocol.REQUEST_TX_ROLLBACK -> new ORollbackTransactionRequest();
      case ChannelBinaryProtocol.REQUEST_DB_OPEN -> new OOpen37Request();
      case ChannelBinaryProtocol.REQUEST_CONNECT -> new OConnect37Request();
      case ChannelBinaryProtocol.REQUEST_DB_REOPEN -> new OReopenRequest();
      case ChannelBinaryProtocol.REQUEST_SHUTDOWN -> new OShutdownRequest();
      case ChannelBinaryProtocol.REQUEST_DB_LIST -> new OListDatabasesRequest();
      case ChannelBinaryProtocol.REQUEST_SERVER_INFO -> new OServerInfoRequest();
      case ChannelBinaryProtocol.REQUEST_DB_RELOAD -> new OReloadRequest37();
      case ChannelBinaryProtocol.REQUEST_DB_CREATE -> new OCreateDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_CLOSE -> new OCloseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_EXIST -> new OExistsDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_DROP -> new ODropDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_SIZE -> new OGetSizeRequest();
      case ChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS -> new OCountRecordsRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER -> new ODistributedStatusRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_COUNT -> new OCountRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE -> new OGetClusterDataRangeRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_ADD -> new OAddClusterRequest();
      case ChannelBinaryProtocol.REQUEST_CLUSTER_DROP -> new ODropClusterRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_METADATA -> new OGetRecordMetadataRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_LOAD -> new OReadRecordRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_CREATE -> new OCreateRecordRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_UPDATE -> new OUpdateRecordRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER -> new OHigherPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_CEILING ->
          new OCeilingPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_LOWER -> new OLowerPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR -> new OFloorPhysicalPositionsRequest();
      case ChannelBinaryProtocol.REQUEST_COMMAND -> new OCommandRequest();
      case ChannelBinaryProtocol.REQUEST_SERVER_QUERY -> new OServerQueryRequest();
      case ChannelBinaryProtocol.REQUEST_QUERY -> new OQueryRequest();
      case ChannelBinaryProtocol.REQUEST_CLOSE_QUERY -> new OCloseQueryRequest();
      case ChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE -> new OQueryNextPageRequest();
      case ChannelBinaryProtocol.REQUEST_CONFIG_GET -> new OGetGlobalConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_CONFIG_SET -> new OSetGlobalConfigurationRequest();
      case ChannelBinaryProtocol.REQUEST_CONFIG_LIST -> new OListGlobalConfigurationsRequest();
      case ChannelBinaryProtocol.REQUEST_DB_FREEZE -> new OFreezeDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_DB_RELEASE -> new OReleaseDatabaseRequest();
      case ChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT -> new OCleanOutRecordRequest();
      case ChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI -> new OSBTCreateTreeRequest();
      case ChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET -> new OSBTGetRequest();
      case ChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY -> new OSBTFirstKeyRequest();
      case ChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR ->
          new OSBTFetchEntriesMajorRequest<>();
      case ChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE -> new OSBTGetRealBagSizeRequest();
      case ChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP -> new OIncrementalBackupRequest();
      case ChannelBinaryProtocol.REQUEST_DB_IMPORT -> new OImportRequest();
      case ChannelBinaryProtocol.DISTRIBUTED_CONNECT -> new ODistributedConnectRequest();
      default -> throw new DatabaseException(
          "binary protocol command with code: " + requestType + " for protocol version 37");
    };
  }

  /**
   * Protocol 38
   */
  public static OBinaryRequest<? extends OBinaryResponse> createRequest38(int requestType) {
    return switch (requestType) {
      case ChannelBinaryProtocol.REQUEST_TX_FETCH -> new OFetchTransaction38Request();
      case ChannelBinaryProtocol.REQUEST_TX_REBEGIN -> new ORebeginTransaction38Request();
      case ChannelBinaryProtocol.REQUEST_TX_BEGIN -> new OBeginTransaction38Request();
      case ChannelBinaryProtocol.REQUEST_SEND_TRANSACTION_STATE ->
          new OSendTransactionStateRequest();
      case ChannelBinaryProtocol.REQUEST_TX_COMMIT -> new OCommit38Request();
      default -> createRequest37(requestType);
    };
  }
}
