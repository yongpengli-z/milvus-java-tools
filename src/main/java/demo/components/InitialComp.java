package demo.components;

import io.milvus.client.MilvusServiceClient;
import io.milvus.param.R;
import io.milvus.param.collection.DropCollectionParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static demo.BaseTest.milvusClient;

/**
 * @Author yongpeng.li @Date 2024/6/4 17:37
 */
@Slf4j
public class InitialComp {
    public static void initialRunning(boolean cleanCollection){
        R<ListCollectionsResponse> listCollectionsResponseR =
                milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
        List<String> collectionNames = listCollectionsResponseR.getData().collectionNames;
        if (cleanCollection) {
            collectionNames.forEach(
                    x ->
                            milvusClient.dropCollection(
                                    DropCollectionParam.newBuilder().withCollectionName(x).build()));
            log.info("clean collections successfully!");
        }
    }
}
