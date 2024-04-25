package demo;


import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.DataType;
import io.milvus.grpc.DescribeCollectionResponse;
import io.milvus.grpc.MutationResult;
import io.milvus.param.*;
import io.milvus.param.collection.*;
import io.milvus.param.dml.InsertParam;
import io.milvus.param.index.CreateIndexParam;
import util.PropertyFilesUtil;

import java.util.*;
import java.util.logging.Logger;


public class InsertCollection {
    public static Logger logger = Logger.getLogger(InsertCollection.class.getName());


    public static void main(String[] args) {
        int dim = Integer.parseInt(PropertyFilesUtil.getRunValue("dim"));
        int shardNum = Integer.parseInt(PropertyFilesUtil.getRunValue("shard_num"));
        int batchSize = Integer.parseInt(PropertyFilesUtil.getRunValue("batch_size"));
        int concurrencyNum = Integer.parseInt(PropertyFilesUtil.getRunValue("concurrency_num"));
        long totalNum = Long.parseLong(PropertyFilesUtil.getRunValue("total_num"));
        boolean cleanCollection = Boolean.parseBoolean(PropertyFilesUtil.getRunValue("clean_collection"));

        // connect to milvus
        final MilvusServiceClient milvusClient = new MilvusServiceClient(
                ConnectParam.newBuilder()
                        .withUri(PropertyFilesUtil.getRunValue("uri"))
                        .withToken(PropertyFilesUtil.getRunValue("token"))
                        .build());
        logger.info("Connecting to DB: " + PropertyFilesUtil.getRunValue("uri"));

        String collectionName = "book";
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

        //insert data with customized ids
        Random ran = new Random();
        int singleNum = 10000;
        int insertRounds = 100;
        long insertTotalTime = 0L;
        logger.info("Inserting " + singleNum * insertRounds + " entities... ");
        for (int r = 0; r < insertRounds; r++) {
            long startTime = System.currentTimeMillis();
            List<Long> book_id_array = new ArrayList<>();
            List<Long> word_count_array = new ArrayList<>();
            List<List<Float>> book_intro_array = new ArrayList<>();
            for (long i = r * singleNum; i < (r + 1) * singleNum; ++i) {
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
            R<MutationResult> insertR = milvusClient.insert(insertParam);
            long endTime = System.currentTimeMillis();
            logger.info("Insert " +batchSize +" cost:" + (endTime - startTime) / 1000.00 + " seconds!");
            insertTotalTime += (endTime - startTime) / 1000.00;
        }
        logger.info("Total cost of inserting " + totalNum + " entities: " + insertTotalTime + " seconds!");
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


        // build index
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
