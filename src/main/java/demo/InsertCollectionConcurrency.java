package demo;

import com.alibaba.fastjson.JSON;
import demo.utils.HttpClientUtils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.*;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import io.milvus.param.index.CreateIndexParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class InsertCollectionConcurrency {
  private static final Logger logger = LoggerFactory.getLogger(InsertCollection.class);

  public static void main(String[] args) throws InterruptedException {
    String uri = System.getProperty("uri") == null ? "172.0.0.1" : System.getProperty("uri");

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
        System.getProperty("collection") == null ? "" : System.getProperty("collection");
    boolean segmentListen =
        (System.getProperty("segment_listen") != null
            && System.getProperty("segment_listen").equalsIgnoreCase("true"));

    // 获取root密码
    String token="";
    String urlPWD = null;
    String substring = uri.substring(uri.indexOf("https://") + 8, 28);
    if (uri.contains("ali")||uri.contains("tc")) {
      urlPWD = "https://cloud-test.cloud-uat.zilliz.cn/cloud/v1/test/getRootPwd?instanceId=" + substring + "";
      String pwdString = HttpClientUtils.doGet(urlPWD);
      token = "root:"+JSON.parseObject(pwdString).getString("Data");
    } else if(uri.contains("aws")||uri.contains("gcp")||uri.contains("az")) {
      urlPWD = "https://cloud-test.cloud-uat3.zilliz.com/cloud/v1/test/getRootPwd?instanceId=" + substring + "";
      String pwdString = HttpClientUtils.doGet(urlPWD);
      token = "root:"+JSON.parseObject(pwdString).getString("Data");
    } else{
      token = "root:Milvus";
    }


    // connect to milvus
    final MilvusServiceClient milvusClient =
        new MilvusServiceClient(ConnectParam.newBuilder().withUri(uri).withToken(token).build());
    logger.info("Connecting to DB: " + uri);
    Random ran = new Random();

    R<ListCollectionsResponse> listCollectionsResponseR =
        milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
    List<String> collectionNames = listCollectionsResponseR.getData().collectionNames;
    if (cleanCollection) {
      collectionNames.forEach(
          x ->
              milvusClient.dropCollection(
                  DropCollectionParam.newBuilder().withCollectionName(x).build()));
      logger.info("clean collections successfully!");
    }

    FieldType bookIdField =
        FieldType.newBuilder()
            .withName("book_id")
            .withDataType(DataType.Int64)
            .withPrimaryKey(true)
            .withAutoID(false)
            .build();
    FieldType wordCountField =
        FieldType.newBuilder().withName("word_count").withDataType(DataType.Int64).build();
    FieldType bookIntroField =
        FieldType.newBuilder()
            .withName("book_intro")
            .withDataType(DataType.FloatVector)
            .withDimension(dim)
            .build();
    if (collectionName.equalsIgnoreCase("")) {
      collectionName = "book" + ran.nextInt(1000);
      // create collection if not exist
      CreateCollectionParam createCollectionParam =
          CreateCollectionParam.newBuilder()
              .withCollectionName(collectionName)
              .withDescription("my first collection")
              .withShardsNum(shardNum)
              .addFieldType(bookIdField)
              //                    .addFieldType(wordCountField)
              .addFieldType(bookIntroField)
              .build();
      logger.info("Creating example collection: " + collectionName);
      logger.info("Schema: " + createCollectionParam);
      milvusClient.createCollection(createCollectionParam);
      logger.info("Success!");
    } else {
      // Check if the collection does not exist
      R<DescribeCollectionResponse> responseR =
          milvusClient.describeCollection(
              DescribeCollectionParam.newBuilder().withCollectionName(collectionName).build());
      if (responseR.getData() == null) {
        logger.info("你输入的collectionName:" + collectionName + " 不存在!");
        return;
      }
    }

    // 是否预load
    if (perLoad) {
      //  build index
      logger.info("Building AutoIndex...");
      final IndexType INDEX_TYPE = IndexType.AUTOINDEX; // IndexType
      long startIndexTime = System.currentTimeMillis();
      R<RpcStatus> indexR =
          milvusClient.createIndex(
              CreateIndexParam.newBuilder()
                  .withCollectionName(collectionName)
                  .withFieldName(bookIntroField.getName())
                  .withIndexType(INDEX_TYPE)
                  .withMetricType(MetricType.L2)
                  .withSyncMode(Boolean.TRUE)
                  .withSyncWaitingInterval(500L)
                  .withSyncWaitingTimeout(30L)
                  .build());
      long endIndexTime = System.currentTimeMillis();
      logger.info("Succeed in " + (endIndexTime - startIndexTime) / 1000.00 + " seconds!");

      // load collection
      logger.info("Loading collection...");
      long startLoadTime = System.currentTimeMillis();
      int loadState = 0;
      do {
        // load
        R<RpcStatus> rpcStatusR =
            milvusClient.loadCollection(
                LoadCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withSyncLoad(false)
                    .build());
        R<GetLoadStateResponse> loadStateResp =
            milvusClient.getLoadState(
                GetLoadStateParam.newBuilder().withCollectionName(collectionName).build());
        loadState = loadStateResp.getData().getState().getNumber();
        Thread.sleep(1000L);
      } while (loadState != LoadState.LoadStateLoaded.getNumber());
      long endLoadTime = System.currentTimeMillis();
      logger.info("Load cost " + (endLoadTime - startLoadTime) / 1000.00 + " seconds");
    }

    // insert data with customized ids
    long insertRounds = totalNum / batchSize;
    float insertTotalTime = 0;
    logger.info("Inserting total " + totalNum + " entities... ");
    long startTimeTotal = System.currentTimeMillis();
    ExecutorService executorService = Executors.newFixedThreadPool(concurrencyNum);
    ArrayList<Future> list = new ArrayList<>();
    // insert data with multiple threads
    for (int c = 0; c < concurrencyNum; c++) {
      int finalE = c;
      String finalCollectionName = collectionName;

      Callable callable =
          () -> {
            long flushedSegment = 0;
            List<Integer> results = new ArrayList<>();
            for (long r = (insertRounds / concurrencyNum) * finalE;
                r < (insertRounds / concurrencyNum) * (finalE + 1);
                r++) {
              long startTime = System.currentTimeMillis();
              List<Long> book_id_array = new ArrayList<>();
              List<Long> word_count_array = new ArrayList<>();
              List<List<Float>> book_intro_array = new ArrayList<>();
              for (long i = r * batchSize; i < (r + 1) * batchSize; ++i) {
                book_id_array.add(i);
                word_count_array.add(i);
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < dim; ++k) {
                  vector.add(ran.nextFloat());
                }
                book_intro_array.add(vector);
              }
              List<InsertParam.Field> fields = new ArrayList<>();
              fields.add(new InsertParam.Field(bookIdField.getName(), book_id_array));
              //                    fields.add(new InsertParam.Field(wordCountField.getName(),
              // word_count_array));
              fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
              InsertParam insertParam =
                  InsertParam.newBuilder()
                      .withCollectionName(finalCollectionName)
                      .withFields(fields)
                      .build();
              try {
                R<MutationResult> insertR = milvusClient.insert(insertParam);

                if (perLoad && segmentListen && insertR.getStatus() == 9) {
                  logger.info("监测到禁写，开启10min等待...");
                  Thread.sleep(1000L * 60 * 10);
                  R<GetPersistentSegmentInfoResponse> segmentInfoResponseR =
                      milvusClient.getPersistentSegmentInfo(
                          GetPersistentSegmentInfoParam.newBuilder()
                              .withCollectionName(finalCollectionName)
                              .build());
                  long count =
                      segmentInfoResponseR.getData().getInfosList().stream()
                          .filter(x -> x.getState().equals(SegmentState.Flushed))
                          .count();
                  if (count == flushedSegment) {
                    break;
                  }
                  flushedSegment =count;
                }
                results.add(insertR.getStatus());
                if (results.stream().filter(x -> x != 0).count() > 10) {
                  break;
                }
              } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
              }
              long endTime = System.currentTimeMillis();
              logger.info(
                  "线程"
                      + finalE
                      + "插入第"
                      + r
                      + "批次数据,Insert "
                      + batchSize
                      + " cost:"
                      + (endTime - startTime) / 1000.00
                      + " seconds,has insert "
                      + ((r - (insertRounds / concurrencyNum) * finalE) + 1) * batchSize);
            }
            return results;
          };
      Future future = executorService.submit(callable);
      list.add(future);
    }
    for (Future future : list) {
      try {
        logger.info("线程返回结果：" + future.get());
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    long endTimeTotal = System.currentTimeMillis();
    insertTotalTime = (float) ((endTimeTotal - startTimeTotal) / 1000.00);
    logger.info(
        "Total cost of inserting " + totalNum + " entities: " + insertTotalTime + " seconds!");
    executorService.shutdown();
    // 实际数据量
    R<GetCollectionStatisticsResponse> collectionStatistics =
        milvusClient.getCollectionStatistics(
            GetCollectionStatisticsParam.newBuilder().withCollectionName(collectionName).build());
    logger.info("当前collection数据量:" + collectionStatistics);

    // flush data
    logger.info("Flushing...");
    long startFlushTime = System.currentTimeMillis();
    milvusClient.flush(
        FlushParam.newBuilder()
            .withCollectionNames(Collections.singletonList(collectionName))
            .withSyncFlush(true)
            .withSyncFlushWaitingInterval(50L)
            .withSyncFlushWaitingTimeout(30L)
            .build());
    long endFlushTime = System.currentTimeMillis();
    System.out.println("Succeed in " + (endFlushTime - startFlushTime) / 1000.00 + " seconds!");

    //        // search
    //        final Integer SEARCH_K = 2;                       // TopK
    //        final String SEARCH_PARAM = "{\"nprobe\":10}";    // Params
    //        List<String> search_output_fields = Arrays.asList("book_id", "word_count");
    //        for (int i = 0; i < 10; i++) {
    //            List<Float> floatList = new ArrayList<>();
    //            for (int k = 0; k < dim; ++k) {
    //                floatList.add(ran.nextFloat());
    //            }
    //            List<List<Float>> search_vectors = Collections.singletonList(floatList);
    //            SearchParam searchParam = SearchParam.newBuilder()
    //                    .withCollectionName(collectionName)
    //                    .withMetricType(MetricType.L2)
    //                    .withOutFields(search_output_fields)
    //                    .withTopK(SEARCH_K)
    //                    .withVectors(search_vectors)
    //                    .withVectorFieldName(bookIntroField.getName())
    //                    .withParams(SEARCH_PARAM)
    //                    .build();
    //            long startSearchTime = System.currentTimeMillis();
    //            R<SearchResults> search = milvusClient.search(searchParam);
    //            long endSearchTime = System.currentTimeMillis();
    //            logger.info("Searching vector: " + search_vectors);
    //            logger.info("Result: " +
    // search.getData().getResults().getFieldsDataList().size());
    //            logger.info("search " + i + " latency: " + (endSearchTime - startSearchTime) /
    // 1000.00 + " seconds");
    //        }

    milvusClient.close();
  }
}
