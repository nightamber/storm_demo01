package xin.mrbear.demo;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * wordcount的驱动类，用来提交任务的。
 */
public class WordCountTopology {
    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        // 通过TopologyBuilder来封装任务信息
        TopologyBuilder topologyBuilder = new TopologyBuilder();
//        设置spout，获取数据
        topologyBuilder.setSpout("readfilespout",new ReadFileSpout(),2);
//        设置splitbolt，对句子进行切割
        topologyBuilder.setBolt("splitbolt",new SplitBolt(),4).shuffleGrouping("readfilespout");
//        设置wordcountbolt，对单词进行统计
        topologyBuilder.setBolt("wordcountBolt",new WordCountBolt(),2).shuffleGrouping("splitbolt");

//        准备一个配置文件
        Config config = new Config();
//        storm中任务提交有两种方式，一种方式是本地模式，另一种是集群模式。
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("wordcount",config,topologyBuilder.createTopology());
//        //在storm集群中，worker是用来分配的资源。如果一个程序没有指定worker数，那么就会使用默认值。
//        config.setNumWorkers(2);
//        //提交到集群
//        StormSubmitter.submitTopology("wordcount1",config,topologyBuilder.createTopology());
    }
}
