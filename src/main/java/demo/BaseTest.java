package demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import demo.Params.CollectionParams;
import demo.Params.InsertParams;
import demo.Params.UpsertParams;
import demo.components.*;
import demo.utils.HttpClientUtils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.param.R;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author yongpeng.li @Date 2024/6/3 17:40
 */
@Slf4j
public class BaseTest {
  private static final Logger logger = LoggerFactory.getLogger(BaseTest.class);
  public static MilvusServiceClient milvusClient = null;

  public static void main(String[] args) {
    String uri =
        System.getProperty("uri") == null
            ? "http://172.0.0.1:19530"
            : System.getProperty("uri");
    String token =
        System.getProperty("token") == null || System.getProperty("token").equals("")
            ? ""
            : System.getProperty("token");
    boolean cleanCollection =
        (System.getProperty("clean_collection") != null
            && System.getProperty("clean_collection").equalsIgnoreCase("true"));

    String collectionParams =
        System.getProperty("collection_params") == null
                || System.getProperty("collection_params").equals("")
            ? ""
            : System.getProperty("collection_params");
    String insertParams =
        System.getProperty("insert_params") == null
                || System.getProperty("insert_params").equals("")
            ? ""
            : System.getProperty("insert_params");
    String upsertParams =
        System.getProperty("upsert_params") == null
                || System.getProperty("upsert_params").equals("")
            ? ""
            : System.getProperty("upsert_params");

    if (token.equals("")) {
      token = MilvusConnect.provideToken(uri);
    }
    milvusClient = MilvusConnect.createMilvusClient(uri, token);
    R<ListCollectionsResponse> listCollectionsResponseR =
        milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
    System.out.println(listCollectionsResponseR);
    // 初始化环境
    InitialComp.initialRunning(cleanCollection);
    // 创建collection
    CollectionParams collectionParamsObj =
        JSONObject.parseObject(collectionParams, CollectionParams.class);
    CollectionComp.collectionInitial(collectionParamsObj);
    // insert
    if (!insertParams.equals("")) {
      InsertParams insertParamsObj = JSONObject.parseObject(insertParams, InsertParams.class);
      InsertComp.insertTest(insertParamsObj, collectionParamsObj);
    }
    // upsert
    if (!upsertParams.equals("")) {
      UpsertParams upsertParamsObj = JSONObject.parseObject(upsertParams, UpsertParams.class);
      UpsertComp.upsertTest(upsertParamsObj, collectionParamsObj);
    }
    milvusClient.close();
  }
}
