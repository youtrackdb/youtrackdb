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
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
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
      case OChannelBinaryProtocol.REQUEST_DB_OPEN -> new OOpenRequest();
      case OChannelBinaryProtocol.REQUEST_CONNECT -> new OConnectRequest();
      case OChannelBinaryProtocol.REQUEST_DB_REOPEN -> new OReopenRequest();
      case OChannelBinaryProtocol.REQUEST_SHUTDOWN -> new OShutdownRequest();
      case OChannelBinaryProtocol.REQUEST_DB_LIST -> new OListDatabasesRequest();
      case OChannelBinaryProtocol.REQUEST_SERVER_INFO -> new OServerInfoRequest();
      case OChannelBinaryProtocol.REQUEST_DB_RELOAD -> new OReloadRequest();
      case OChannelBinaryProtocol.REQUEST_DB_CREATE -> new OCreateDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_CLOSE -> new OCloseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_EXIST -> new OExistsDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_DROP -> new ODropDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_SIZE -> new OGetSizeRequest();
      case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS -> new OCountRecordsRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER -> new ODistributedStatusRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_COUNT -> new OCountRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE -> new OGetClusterDataRangeRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_ADD -> new OAddClusterRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_DROP -> new ODropClusterRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_METADATA -> new OGetRecordMetadataRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD -> new OReadRecordRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_EXISTS -> new ORecordExistsRequest();
      case OChannelBinaryProtocol.REQUEST_SEND_TRANSACTION_STATE ->
          new OSendTransactionStateRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_CREATE -> new OCreateRecordRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE -> new OUpdateRecordRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER -> new OHigherPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING ->
          new OCeilingPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER -> new OLowerPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR -> new OFloorPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_COMMAND -> new OCommandRequest();
      case OChannelBinaryProtocol.REQUEST_SERVER_QUERY -> new OServerQueryRequest();
      case OChannelBinaryProtocol.REQUEST_QUERY -> new OQueryRequest();
      case OChannelBinaryProtocol.REQUEST_CLOSE_QUERY -> new OCloseQueryRequest();
      case OChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE -> new OQueryNextPageRequest();
      case OChannelBinaryProtocol.REQUEST_TX_COMMIT -> new OCommitRequest();
      case OChannelBinaryProtocol.REQUEST_CONFIG_GET -> new OGetGlobalConfigurationRequest();
      case OChannelBinaryProtocol.REQUEST_CONFIG_SET -> new OSetGlobalConfigurationRequest();
      case OChannelBinaryProtocol.REQUEST_CONFIG_LIST -> new OListGlobalConfigurationsRequest();
      case OChannelBinaryProtocol.REQUEST_DB_FREEZE -> new OFreezeDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_RELEASE -> new OReleaseDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT -> new OCleanOutRecordRequest();
      case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI -> new OSBTCreateTreeRequest();
      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET -> new OSBTGetRequest();
      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY -> new OSBTFirstKeyRequest();
      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR ->
          new OSBTFetchEntriesMajorRequest<>();
      case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE -> new OSBTGetRealBagSizeRequest();
      case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP -> new OIncrementalBackupRequest();
      case OChannelBinaryProtocol.REQUEST_DB_IMPORT -> new OImportRequest();
      case OChannelBinaryProtocol.DISTRIBUTED_CONNECT -> new ODistributedConnectRequest();
      default -> throw new YTDatabaseException("binary protocol command with code: " + requestType);
    };
  }

  /**
   * Protocol 37
   */
  public static OBinaryRequest<? extends OBinaryResponse> createRequest37(int requestType) {
    return switch (requestType) {
      case OChannelBinaryProtocol.SUBSCRIBE_PUSH -> new OSubscribeRequest();
      case OChannelBinaryProtocol.UNSUBSCRIBE_PUSH -> new OUnsubscribeRequest();
      case OChannelBinaryProtocol.REQUEST_TX_FETCH -> new OFetchTransactionRequest();
      case OChannelBinaryProtocol.REQUEST_TX_REBEGIN -> new ORebeginTransactionRequest();
      case OChannelBinaryProtocol.REQUEST_TX_BEGIN -> new OBeginTransactionRequest();
      case OChannelBinaryProtocol.REQUEST_TX_COMMIT -> new OCommit37Request();
      case OChannelBinaryProtocol.REQUEST_TX_ROLLBACK -> new ORollbackTransactionRequest();
      case OChannelBinaryProtocol.REQUEST_DB_OPEN -> new OOpen37Request();
      case OChannelBinaryProtocol.REQUEST_CONNECT -> new OConnect37Request();
      case OChannelBinaryProtocol.REQUEST_DB_REOPEN -> new OReopenRequest();
      case OChannelBinaryProtocol.REQUEST_SHUTDOWN -> new OShutdownRequest();
      case OChannelBinaryProtocol.REQUEST_DB_LIST -> new OListDatabasesRequest();
      case OChannelBinaryProtocol.REQUEST_SERVER_INFO -> new OServerInfoRequest();
      case OChannelBinaryProtocol.REQUEST_DB_RELOAD -> new OReloadRequest37();
      case OChannelBinaryProtocol.REQUEST_DB_CREATE -> new OCreateDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_CLOSE -> new OCloseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_EXIST -> new OExistsDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_DROP -> new ODropDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_SIZE -> new OGetSizeRequest();
      case OChannelBinaryProtocol.REQUEST_DB_COUNTRECORDS -> new OCountRecordsRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER -> new ODistributedStatusRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_COUNT -> new OCountRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_DATARANGE -> new OGetClusterDataRangeRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_ADD -> new OAddClusterRequest();
      case OChannelBinaryProtocol.REQUEST_CLUSTER_DROP -> new ODropClusterRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_METADATA -> new OGetRecordMetadataRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_LOAD -> new OReadRecordRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_CREATE -> new OCreateRecordRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_UPDATE -> new OUpdateRecordRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_HIGHER -> new OHigherPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_CEILING ->
          new OCeilingPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_LOWER -> new OLowerPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_POSITIONS_FLOOR -> new OFloorPhysicalPositionsRequest();
      case OChannelBinaryProtocol.REQUEST_COMMAND -> new OCommandRequest();
      case OChannelBinaryProtocol.REQUEST_SERVER_QUERY -> new OServerQueryRequest();
      case OChannelBinaryProtocol.REQUEST_QUERY -> new OQueryRequest();
      case OChannelBinaryProtocol.REQUEST_CLOSE_QUERY -> new OCloseQueryRequest();
      case OChannelBinaryProtocol.REQUEST_QUERY_NEXT_PAGE -> new OQueryNextPageRequest();
      case OChannelBinaryProtocol.REQUEST_CONFIG_GET -> new OGetGlobalConfigurationRequest();
      case OChannelBinaryProtocol.REQUEST_CONFIG_SET -> new OSetGlobalConfigurationRequest();
      case OChannelBinaryProtocol.REQUEST_CONFIG_LIST -> new OListGlobalConfigurationsRequest();
      case OChannelBinaryProtocol.REQUEST_DB_FREEZE -> new OFreezeDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_DB_RELEASE -> new OReleaseDatabaseRequest();
      case OChannelBinaryProtocol.REQUEST_RECORD_CLEAN_OUT -> new OCleanOutRecordRequest();
      case OChannelBinaryProtocol.REQUEST_CREATE_SBTREE_BONSAI -> new OSBTCreateTreeRequest();
      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET -> new OSBTGetRequest();
      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_FIRST_KEY -> new OSBTFirstKeyRequest();
      case OChannelBinaryProtocol.REQUEST_SBTREE_BONSAI_GET_ENTRIES_MAJOR ->
          new OSBTFetchEntriesMajorRequest<>();
      case OChannelBinaryProtocol.REQUEST_RIDBAG_GET_SIZE -> new OSBTGetRealBagSizeRequest();
      case OChannelBinaryProtocol.REQUEST_INCREMENTAL_BACKUP -> new OIncrementalBackupRequest();
      case OChannelBinaryProtocol.REQUEST_DB_IMPORT -> new OImportRequest();
      case OChannelBinaryProtocol.DISTRIBUTED_CONNECT -> new ODistributedConnectRequest();
      default -> throw new YTDatabaseException(
          "binary protocol command with code: " + requestType + " for protocol version 37");
    };
  }

  /**
   * Protocol 38
   */
  public static OBinaryRequest<? extends OBinaryResponse> createRequest38(int requestType) {
    return switch (requestType) {
      case OChannelBinaryProtocol.REQUEST_TX_FETCH -> new OFetchTransaction38Request();
      case OChannelBinaryProtocol.REQUEST_TX_REBEGIN -> new ORebeginTransaction38Request();
      case OChannelBinaryProtocol.REQUEST_TX_BEGIN -> new OBeginTransaction38Request();
      case OChannelBinaryProtocol.REQUEST_SEND_TRANSACTION_STATE ->
          new OSendTransactionStateRequest();
      case OChannelBinaryProtocol.REQUEST_TX_COMMIT -> new OCommit38Request();
      default -> createRequest37(requestType);
    };
  }
}
