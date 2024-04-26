package demo;


import com.google.common.collect.Lists;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;


public class InsertCollectionConcurrency {
    private static final Logger logger = LoggerFactory.getLogger(InsertCollection.class);


    public static void main(String[] args) {
        String uri = System.getProperty("uri") == null ? "172.0.0.1" : System.getProperty("uri");
        String token = System.getProperty("token") == null ? "default" : System.getProperty("token");
        int dim = System.getProperty("dim") == null ? 768 : Integer.parseInt(System.getProperty("dim"));
        int shardNum = System.getProperty("shard_num") == null ? 1 : Integer.parseInt(System.getProperty("shard_num"));
        int batchSize = System.getProperty("batch_size") == null ? 10000 : Integer.parseInt(System.getProperty("batch_size"));
        int concurrencyNum = System.getProperty("concurrency_num") == null ? 1 : Integer.parseInt(System.getProperty("concurrency_num"));
        long totalNum =  System.getProperty("total_num") == null ? 10000 : Integer.parseInt(System.getProperty("total_num"));
        boolean cleanCollection = System.getProperty("clean_collection") != null && Boolean.getBoolean(System.getProperty("clean_collection"));
        boolean perLoad = System.getProperty("perLoad") != null && Boolean.getBoolean(System.getProperty("perLoad"));

        logger.info("perLoad: " + perLoad);
        // connect to milvus
        final MilvusServiceClient milvusClient = new MilvusServiceClient(
                ConnectParam.newBuilder()
                        .withUri(uri)
                        .withToken(token)
                        .build());
        logger.info("Connecting to DB: " + uri);
        Random ran = new Random();
        String collectionName = "book"+ran.nextInt(1000);
        if (cleanCollection) {
            milvusClient.dropCollection(DropCollectionParam.newBuilder().withCollectionName(collectionName).build());
            logger.info("clean collection successfully!");
        }
        // Check if the collection exists
        R<DescribeCollectionResponse> responseR =
                milvusClient.describeCollection(DescribeCollectionParam.newBuilder().withCollectionName(collectionName).build());
        // create collection if not exist
        FieldType bookIdField = FieldType.newBuilder()
                .withName("book_id")
                .withDataType(DataType.Int64)
                .withPrimaryKey(true)
                .withAutoID(false)
                .build();
        FieldType wordCountField = FieldType.newBuilder()
                .withName("word_count")
                .withDataType(DataType.Int64)
                .build();
        FieldType bookIntroField = FieldType.newBuilder()
                .withName("book_intro")
                .withDataType(DataType.FloatVector)
                .withDimension(dim)
                .build();
        if (responseR.getData() == null) {
            CreateCollectionParam createCollectionParam = CreateCollectionParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withDescription("my first collection")
                    .withShardsNum(shardNum)
                    .addFieldType(bookIdField)
                    .addFieldType(wordCountField)
                    .addFieldType(bookIntroField)
                    .build();
            logger.info("Creating example collection: " + collectionName);
            logger.info("Schema: " + createCollectionParam);
            milvusClient.createCollection(createCollectionParam);
            logger.info("Success!");
        }

        if (perLoad){
            //  build index
        logger.info("Building AutoIndex...");
        final IndexType INDEX_TYPE = IndexType.AUTOINDEX;   // IndexType
        long startIndexTime = System.currentTimeMillis();
        R<RpcStatus> indexR = milvusClient.createIndex(
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
        milvusClient.loadCollection(LoadCollectionParam.newBuilder()
                .withCollectionName(collectionName)
                .withSyncLoad(true)
                .withSyncLoadWaitingInterval(500L)
                .withSyncLoadWaitingTimeout(100L)
                .build());
        long endLoadTime = System.currentTimeMillis();
        logger.info("Succeed in " + (endLoadTime - startLoadTime) / 1000.00 + " seconds");

        }

        //insert data with customized ids

        long insertRounds = totalNum/batchSize;
        float insertTotalTime = 0;
        logger.info("Inserting total " + totalNum + " entities... ");
        long startTimeTotal = System.currentTimeMillis();
        ExecutorService executorService = Executors.newFixedThreadPool(concurrencyNum);
        ArrayList<Future> list = new ArrayList<>();
        for(int c = 0; c < concurrencyNum; c++) {
            int finalE = c;
            Callable callable = () -> {
                List<Integer> results = new ArrayList<>();
        for (long r = (insertRounds/concurrencyNum)*finalE; r < (insertRounds/concurrencyNum)*(finalE+1); r++) {
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
            fields.add(new InsertParam.Field(wordCountField.getName(), word_count_array));
            fields.add(new InsertParam.Field(bookIntroField.getName(), book_intro_array));
            InsertParam insertParam = InsertParam.newBuilder()
                    .withCollectionName(collectionName)
                    .withFields(fields)
                    .build();
            try {
                R<MutationResult> insertR = milvusClient.insert(insertParam);
                results.add(insertR.getStatus());
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage());
            }
            long endTime = System.currentTimeMillis();
            logger.info("线程" + finalE + "插入第" + r + "批次数据,Insert " +batchSize +" cost:" + (endTime - startTime) / 1000.00 + " seconds,has insert "+((r-(insertRounds/concurrencyNum)*finalE)+1)*batchSize);

        }
            return results;
        };
            Future future = executorService.submit(callable);
            list.add(future);
        }
        for(Future future : list){
            try {
                logger.info("线程返回结果："+ future.get() );
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        long endTimeTotal = System.currentTimeMillis();
        insertTotalTime = (float) ((endTimeTotal - startTimeTotal) / 1000.00);
        logger.info("Total cost of inserting " + totalNum + " entities: " + insertTotalTime + " seconds!");
        executorService.shutdown();
        // flush data
        logger.info("Flushing...");
        long startFlushTime = System.currentTimeMillis();
        milvusClient.flush(FlushParam.newBuilder()
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
//            logger.info("Result: " + search.getData().getResults().getFieldsDataList().size());
//            logger.info("search " + i + " latency: " + (endSearchTime - startSearchTime) / 1000.00 + " seconds");
//        }

        milvusClient.close();
    }

}
