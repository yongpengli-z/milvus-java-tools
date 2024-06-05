package demo.components;

import com.alibaba.fastjson.JSON;
import demo.BaseTest;
import demo.utils.HttpClientUtils;
import io.milvus.client.MilvusServiceClient;
import io.milvus.grpc.ShowCollectionsResponse;
import io.milvus.param.ConnectParam;
import io.milvus.param.R;
import io.milvus.param.collection.ShowCollectionsParam;
import io.milvus.param.highlevel.collection.ListCollectionsParam;
import io.milvus.param.highlevel.collection.response.ListCollectionsResponse;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author yongpeng.li @Date 2024/6/4 15:17
 */
@Slf4j
public class MilvusConnect {
    public static String provideToken(String uri) {
        // 获取root密码
        String token = "";
        String urlPWD = null;
        if (uri.contains("ali") || uri.contains("tc")) {
            String substring = uri.substring(uri.indexOf("https://") + 8, 28);
            urlPWD =
                    "https://cloud-test.cloud-uat.zilliz.cn/cloud/v1/test/getRootPwd?instanceId="
                            + substring
                            + "";
            String pwdString = HttpClientUtils.doGet(urlPWD);
            token = "root:" + JSON.parseObject(pwdString).getString("Data");
        } else if (uri.contains("aws") || uri.contains("gcp") || uri.contains("az")) {
            String substring = uri.substring(uri.indexOf("https://") + 8, 28);
            urlPWD =
                    "https://cloud-test.cloud-uat3.zilliz.com/cloud/v1/test/getRootPwd?instanceId="
                            + substring
                            + "";
            String pwdString = HttpClientUtils.doGet(urlPWD);
            token = "root:" + JSON.parseObject(pwdString).getString("Data");
        } else {
            token = "root:Milvus";
        }
        return token;
    }

    public static MilvusServiceClient createMilvusClient(String uri, String token){
        MilvusServiceClient milvusClient =
                new MilvusServiceClient(ConnectParam.newBuilder().withUri(uri).withToken(token).build());
        log.info("Connecting to DB: " + uri);
        R<ListCollectionsResponse> listCollectionsResponseR = milvusClient.listCollections(ListCollectionsParam.newBuilder().build());
        log.info("Collection list:"+listCollectionsResponseR.getData().collectionNames);
        return milvusClient;
    }

}
