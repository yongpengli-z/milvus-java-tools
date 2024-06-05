package demo.components;

import demo.Params.CollectionParams;
import demo.Params.InsertParams;
import io.milvus.grpc.GetCollectionStatisticsResponse;
import io.milvus.grpc.GetPersistentSegmentInfoResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.grpc.SegmentState;
import io.milvus.param.R;
import io.milvus.param.collection.FlushParam;
import io.milvus.param.collection.GetCollectionStatisticsParam;
import io.milvus.param.control.GetPersistentSegmentInfoParam;
import io.milvus.param.dml.InsertParam;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;

import static demo.BaseTest.milvusClient;

/**
 * @Author yongpeng.li @Date 2024/6/4 10:34
 */
@Slf4j
public class InsertComp {

  public static void insertTest(InsertParams insertParams, CollectionParams collectionParams) {
    Random ran = new Random();
    long insertRounds = insertParams.getTotalNum() / insertParams.getBatchSize();
    float insertTotalTime = 0;
    log.info("Inserting total " + insertParams.getTotalNum() + " entities... ");
    long startTimeTotal = System.currentTimeMillis();
    ExecutorService executorService =
        Executors.newFixedThreadPool(insertParams.getConcurrencyNum());
    ArrayList<Future<List<Integer>>> list = new ArrayList<>();
    // insert data with multiple threads
    for (int c = 0; c < insertParams.getConcurrencyNum(); c++) {
      int finalE = c;
      String finalCollectionName = collectionParams.getCollectionName();

      Callable callable =
          () -> {
            List<Integer> results = new ArrayList<>();
            for (long r = (insertRounds / insertParams.getConcurrencyNum()) * finalE;
                r < (insertRounds / insertParams.getConcurrencyNum()) * (finalE + 1);
                r++) {
              long startTime = System.currentTimeMillis();
              List<Long> book_id_array = new ArrayList<>();
              List<Long> word_count_array = new ArrayList<>();
              List<List<Float>> book_intro_array = new ArrayList<>();
              for (long i = r * insertParams.getBatchSize();
                  i < (r + 1) * insertParams.getBatchSize();
                  ++i) {
                book_id_array.add(i);
                word_count_array.add(i);
                List<Float> vector = new ArrayList<>();
                for (int k = 0; k < collectionParams.getDim(); ++k) {
                  vector.add(ran.nextFloat());
                }
                book_intro_array.add(vector);
              }
              List<InsertParam.Field> fields = new ArrayList<>();
              fields.add(new InsertParam.Field("book_id", book_id_array));
              fields.add(new InsertParam.Field("word_count", word_count_array));
              fields.add(new InsertParam.Field("book_intro", book_intro_array));
              InsertParam insertParam =
                  InsertParam.newBuilder()
                      .withCollectionName(finalCollectionName)
                      .withFields(fields)
                      .build();
              try {
                R<MutationResult> insertR = milvusClient.insert(insertParam);

                if (collectionParams.isPerLoad()
                    && insertParams.isSegmentListen()
                    && insertR.getStatus() == 9) {
                  log.info("监测到禁写，开启15min等待...");
                  R<GetPersistentSegmentInfoResponse> segmentInfoResponseR0 =
                      milvusClient.getPersistentSegmentInfo(
                          GetPersistentSegmentInfoParam.newBuilder()
                              .withCollectionName(finalCollectionName)
                              .build());
                  long count0 =
                      segmentInfoResponseR0.getData().getInfosList().stream()
                          .filter(x -> x.getState().equals(SegmentState.Flushed))
                          .count();
                  Thread.sleep(1000L * 60 * 15);
                  R<GetPersistentSegmentInfoResponse> segmentInfoResponseR =
                      milvusClient.getPersistentSegmentInfo(
                          GetPersistentSegmentInfoParam.newBuilder()
                              .withCollectionName(finalCollectionName)
                              .build());
                  long count =
                      segmentInfoResponseR.getData().getInfosList().stream()
                          .filter(x -> x.getState().equals(SegmentState.Flushed))
                          .count();
                  if (count0 == count) {
                    break;
                  }
                }
                if (!insertParams.isSegmentListen()) {
                  results.add(insertR.getStatus());
                  if (results.stream().filter(x -> x != 0).count() > 10) {
                    break;
                  }
                }
              } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
              }
              long endTime = System.currentTimeMillis();
              log.info(
                  "线程"
                      + finalE
                      + "插入第"
                      + r
                      + "批次数据,Insert "
                      + insertParams.getBatchSize()
                      + " cost:"
                      + (endTime - startTime) / 1000.00
                      + " seconds,has insert "
                      + ((r - (insertRounds / insertParams.getConcurrencyNum()) * finalE) + 1)
                          * insertParams.getBatchSize());
            }
            return results;
          };
      Future<List<Integer>> future = executorService.submit(callable);
      list.add(future);
    }
    long requestNum = 0;
    for (Future<List<Integer>> future : list) {
      try {
        long count = future.get().stream().filter(x -> x == 0).count();
        log.info("线程返回结果：" + future.get());
        requestNum += count;
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }
    long endTimeTotal = System.currentTimeMillis();
    insertTotalTime = (float) ((endTimeTotal - startTimeTotal) / 1000.00);
    log.info(
        "Total cost of inserting "
            + insertParams.getTotalNum()
            + " entities: "
            + insertTotalTime
            + " seconds!");
    log.info("Total insert " + requestNum + " 次数,RPS avg :" + requestNum / insertTotalTime);
    executorService.shutdown();
    // flush data
    log.info("Flushing...");
    long startFlushTime = System.currentTimeMillis();
    milvusClient.flush(
        FlushParam.newBuilder()
            .withCollectionNames(Collections.singletonList(collectionParams.getCollectionName()))
            .withSyncFlush(true)
            .withSyncFlushWaitingInterval(50L)
            .withSyncFlushWaitingTimeout(30L)
            .build());
    long endFlushTime = System.currentTimeMillis();
    System.out.println("Succeed in " + (endFlushTime - startFlushTime) / 1000.00 + " seconds!");

    // 实际数据量
    R<GetCollectionStatisticsResponse> collectionStatistics =
        milvusClient.getCollectionStatistics(
            GetCollectionStatisticsParam.newBuilder()
                .withCollectionName(collectionParams.getCollectionName())
                .build());
    log.info("当前collection["+collectionParams.getCollectionName()+"]数据量:" + collectionStatistics);
  }
}
