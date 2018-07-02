package xin.mrbear.demo;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * 输入：单词及单词出现的次数
 * 输出：打印在控制台
 * 负责统计每个单词出现的次数
 * 类似于hadoop MapReduce中的reduce函数
 */
public class WordCountBolt extends BaseRichBolt {
    private Map<String, Integer> wordCountMap = new HashMap<String, Integer>();

    /**
     * 初始化方法
     *
     * @param stormConf 集群及用户自定义的配置文件
     * @param context   上下文对象
     * @param collector 数据收集器
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // 由于wordcount是最后一个bolt，所有不需要自定义OutputCollector collector，并赋值。
    }

    @Override
    public void execute(Tuple input) {
        //获取单词出现的信息（单词、次数）
        String word = input.getStringByField("word");
        String num = input.getStringByField("num");
        // 定义map记录单词出现的次数
        // 开始计数
        if (wordCountMap.containsKey(word)) {
            Integer integer = wordCountMap.get(word);
            wordCountMap.put(word, integer + Integer.parseInt(num));
        } else {
            wordCountMap.put(word, Integer.parseInt(num));
        }
        // 打印整个map
        System.out.println(wordCountMap);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // 不发送数据，所以不用实现。
    }
}
