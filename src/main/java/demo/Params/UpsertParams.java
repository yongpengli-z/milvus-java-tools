package demo.Params;

import lombok.Data;

/**
 * @Author yongpeng.li @Date 2024/6/4 15:05
 */
@Data
public class UpsertParams {
    private int batchSize;
    private  int concurrencyNum;
    private long totalNum;
    private boolean segmentListen;
    private int step;
}
