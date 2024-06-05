package demo.Params;

import lombok.Data;

/**
 * @Author yongpeng.li @Date 2024/6/4 16:57
 */
@Data
public class CollectionParams {
    private String collectionName;
    private int dim;
    private int shardNum;
    private boolean perLoad;
    private boolean partitionKey;

}
