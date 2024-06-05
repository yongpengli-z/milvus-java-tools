package demo.components;

import demo.Params.CollectionParams;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.GetLoadStateResponse;
import io.milvus.grpc.LoadState;
import io.milvus.param.IndexType;
import io.milvus.param.MetricType;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.index.CreateIndexParam;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Random;

import static demo.BaseTest.milvusClient;

/**
 * @Author yongpeng.li @Date 2024/6/4 17:29
 */
@Slf4j
public class CollectionComp {
    public static void collectionInitial(CollectionParams collectionParams){
        Random ran = new Random();
        FieldType bookIdField =
                FieldType.newBuilder()
                        .withName("book_id")
                        .withDataType(DataType.Int64)
                        .withPrimaryKey(true)
                        .withAutoID(false)
                        .build();
        FieldType wordCountField =
                FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).withPartitionKey(collectionParams.isPartitionKey()).build();
        FieldType bookIntroField =
                FieldType.newBuilder()
                        .withName("book_intro")
                        .withDataType(DataType.FloatVector)
                        .withDimension(collectionParams.getDim())
                        .build();
        if (collectionParams.getCollectionName().equalsIgnoreCase("")) {
            collectionParams.setCollectionName( "book" + ran.nextInt(1000));
            // create collection if not exist
            CreateCollectionParam createCollectionParam =
                    CreateCollectionParam.newBuilder()
                            .withCollectionName(collectionParams.getCollectionName())
                            .withDescription("my first collection")
                            .withShardsNum(collectionParams.getShardNum())
                            .addFieldType(bookIdField)
                            .addFieldType(wordCountField)
                            .addFieldType(bookIntroField)
                            .build();
            log.info("Creating example collection: " + collectionParams.getCollectionName());
            log.info("Schema: " + createCollectionParam);
            milvusClient.createCollection(createCollectionParam);
            log.info("Success!");
        } else {
            // Check if the collection does not exist
            R<DescribeCollectionResponse> responseR =
                    milvusClient.describeCollection(
                            DescribeCollectionParam.newBuilder().withCollectionName(collectionParams.getCollectionName()).build());
            if (responseR.getData() == null) {
                log.info("你输入的collectionName:" + collectionParams.getCollectionName() + "未找到，将为你创建。");
                // create collection if not exist
                CreateCollectionParam createCollectionParam =
                        CreateCollectionParam.newBuilder()
                                .withCollectionName(collectionParams.getCollectionName())
                                .withDescription("my first collection")
                                .withShardsNum(collectionParams.getShardNum())
                                .addFieldType(bookIdField)
                                .addFieldType(wordCountField)
                                .addFieldType(bookIntroField)
                                .build();
                log.info("Creating example collection: " + collectionParams.getCollectionName());
                log.info("Schema: " + createCollectionParam);
                milvusClient.createCollection(createCollectionParam);
                log.info("Success!");
            }
        }

        //  build index
        log.info("Building AutoIndex...");
        final IndexType INDEX_TYPE = IndexType.AUTOINDEX; // IndexType
        long startIndexTime = System.currentTimeMillis();
        R<RpcStatus> indexR =
                milvusClient.createIndex(
                        CreateIndexParam.newBuilder()
                                .withCollectionName(collectionParams.getCollectionName())
                                .withFieldName(bookIntroField.getName())
                                .withIndexType(INDEX_TYPE)
                                .withMetricType(MetricType.L2)
                                .withSyncMode(Boolean.TRUE)
                                .withSyncWaitingInterval(500L)
                                .withSyncWaitingTimeout(30L)
                                .build());
        long endIndexTime = System.currentTimeMillis();
        log.info("Succeed in " + (endIndexTime - startIndexTime) / 1000.00 + " seconds!");

        // 是否预load
        if (collectionParams.isPerLoad()) {

            // load collection
            log.info("Loading collection...");
            long startLoadTime = System.currentTimeMillis();
            int loadState = 0;
            // load
            R<RpcStatus> rpcStatusR =
                    milvusClient.loadCollection(
                            LoadCollectionParam.newBuilder()
                                    .withCollectionName(collectionParams.getCollectionName())
                                    .withSyncLoad(false)
                                    .build());
            do {
                R<GetLoadStateResponse> loadStateResp =
                        milvusClient.getLoadState(
                                GetLoadStateParam.newBuilder().withCollectionName(collectionParams.getCollectionName()).build());
                loadState = loadStateResp.getData().getState().getNumber();
                try {
                    Thread.sleep(1000L);
                } catch (InterruptedException e) {
                    log.error(e.getMessage());
                }
            } while (loadState != LoadState.LoadStateLoaded.getNumber());
            long endLoadTime = System.currentTimeMillis();
            log.info("Load cost " + (endLoadTime - startLoadTime) / 1000.00 + " seconds");
        }
    }
}
