package demo;

import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.GetLoadStateResponse;
import io.milvus.grpc.LoadState;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.RpcStatus;
import io.milvus.param.collection.*;
import io.milvus.param.dml.DeleteParam;
import io.milvus.param.highlevel.dml.DeleteIdsParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @Author yongpeng.li @Date 2024/4/29 10:55
 */
public class LoadReleaseCollection {
  private static final Logger logger = LoggerFactory.getLogger(LoadReleaseCollection.class);

  public static void main(String[] args) throws InterruptedException {

    String uri = "https://in01-e83dc86a0b37ac1.aws-us-west-2.vectordb-uat3.zillizcloud.com:19532";
    String token =
        "6aff239bed5702130e09ad03a3379a71034a3e7b4160de384dce58c501e5bf98e49816c670b4b6335f51276c294976ce6ba25fa4";

    int dim = System.getProperty("dim") == null ? 768 : Integer.parseInt(System.getProperty("dim"));
    int shardNum =
        System.getProperty("shard_num") == null
            ? 1
            : Integer.parseInt(System.getProperty("shard_num"));
    int batchSize =
        System.getProperty("batch_size") == null
            ? 10000
            : Integer.parseInt(System.getProperty("batch_size"));
    int concurrencyNum =
        System.getProperty("concurrency_num") == null
            ? 1
            : Integer.parseInt(System.getProperty("concurrency_num"));
    long totalNum =
        System.getProperty("total_num") == null
            ? 10000
            : Integer.parseInt(System.getProperty("total_num"));
    boolean cleanCollection =
        (System.getProperty("clean_collection") != null
            && System.getProperty("clean_collection").equalsIgnoreCase("true"));
    boolean perLoad =
        System.getProperty("perload") != null
            && System.getProperty("perload").equalsIgnoreCase("true");
    String collectionName =
        System.getProperty("collection") == null ? "book125" : System.getProperty("collection");
    // connect to milvus

    final MilvusServiceClient milvusClient =
        new MilvusServiceClient(
            ConnectParam.newBuilder()
                .withUri(uri)
                .withToken(token)
                .keepAliveWithoutCalls(true)
                .withConnectTimeout(12L, TimeUnit.HOURS)
                .withKeepAliveTimeout(12L, TimeUnit.HOURS)
                .build());
    logger.info("Connecting to DB: " + uri);
    // Check if the collection does not exist
    R<DescribeCollectionResponse> responseR =
        milvusClient.describeCollection(
            DescribeCollectionParam.newBuilder().withCollectionName(collectionName).build());
    if (responseR.getData() == null) {
      logger.info("你输入的collectionName:" + collectionName + " 不存在!");
      return;
    }
    // 实际数据量
    R<GetCollectionStatisticsResponse> collectionStatistics =
        milvusClient.getCollectionStatistics(
            GetCollectionStatisticsParam.newBuilder().withCollectionName(collectionName).build());
    logger.info("当前collection数据量:" + collectionStatistics);

    List<Float> loadTimes = new ArrayList<>();

    for (int i = 0; i < 11; i++) {

      long startLoadTime = System.currentTimeMillis();
      int loadState = 0;
      // load
      R<RpcStatus> rpcStatusR =
              milvusClient.loadCollection(
                      LoadCollectionParam.newBuilder()
                              .withCollectionName(collectionName)
                              .withSyncLoad(true)
                              .withSyncLoadWaitingTimeout(30L)
                              .build());
      logger.info("Load resp:"+rpcStatusR);
      do {
        R<GetLoadStateResponse> loadStateResp =
            milvusClient.getLoadState(
                GetLoadStateParam.newBuilder().withCollectionName(collectionName).build());
        loadState = loadStateResp.getData().getState().getNumber();
        Thread.sleep(1000L);
      } while (loadState != LoadState.LoadStateLoaded.getNumber());
      long endLoadTime = System.currentTimeMillis();
      float loadCostTime = (float) ((endLoadTime - startLoadTime) / 1000.00);
      logger.info("load cost:" + loadCostTime + "s");
      loadTimes.add(loadCostTime);
      Thread.sleep(60*1000L);
      // release
      R<RpcStatus> releaseResp =
          milvusClient.releaseCollection(
              ReleaseCollectionParam.newBuilder().withCollectionName(collectionName).build());
      logger.info("release:" + releaseResp.getData() + ",waiting for " + (loadCostTime>120?120:loadCostTime) + "s");
      try {
        Thread.sleep((long) ((loadCostTime>120?120:loadCostTime )* 1000L));
      } catch (InterruptedException e) {
        logger.error(e.getMessage());
      }
    }
    // 统计结果
    loadTimes.remove(0);
    logger.info("10次load时间:" + loadTimes);
    logger.info("平均耗时:" + loadTimes.stream().mapToDouble(Float::doubleValue).average().orElse(0));
  }
}
