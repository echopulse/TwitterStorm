package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.spout.TwitterSampleSpout;

// Created by gperinazzo on 28/11/2014

public class TwitterTest {

    public static class PrinterTweetBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            System.out.println(tuple.getString(0));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer ofd) {

        }
    }

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new TwitterSampleSpout(), 1);
        builder.setBolt("split", new SplitBolt(), 3).shuffleGrouping("spout");

        // Hashtags
        builder.setBolt("hashtag-counter", new RollingCountBolt(9, 3), 3).fieldsGrouping("split", "hashtags", new Fields("hashtag"));
        builder.setBolt("hashtag-intermediate-ranking", new IntermediateRankingsBolt(10), 3).fieldsGrouping("hashtag-counter", new Fields("obj"));
        builder.setBolt("hashtag-total-ranking", new TotalRankingsBolt(10)).globalGrouping("hashtag-intermediate-ranking");
        builder.setBolt("hashtag-ranking-print", new RankingPrinterBolt("HASHTAG_RANKING.txt")).shuffleGrouping("hashtag-total-ranking");

        // Most active user
        builder.setBolt("user-counter", new RollingCountBolt(9, 3), 3).fieldsGrouping("split", "usernames", new Fields("username"));
        builder.setBolt("user-intermediate-ranking", new IntermediateRankingsBolt(10), 3).fieldsGrouping("user-counter", new Fields("obj"));
        builder.setBolt("user-total-ranking", new TotalRankingsBolt(10)).globalGrouping("user-intermediate-ranking");
        builder.setBolt("user-ranking-print", new RankingPrinterBolt("USER_RANKING.txt")).shuffleGrouping("user-total-ranking");

        // Mentions
        builder.setBolt("mentions-counter", new RollingCountBolt(9, 3), 3).fieldsGrouping("split", "mentions", new Fields("mention"));
        builder.setBolt("mentions-intermediate-ranking", new IntermediateRankingsBolt(10), 3).fieldsGrouping("mentions-counter", new Fields("obj"));
        builder.setBolt("mentions-total-ranking", new TotalRankingsBolt(10)).globalGrouping("mentions-intermediate-ranking");
        builder.setBolt("mentions-ranking-print", new RankingPrinterBolt("MENTIONS_RANKING.txt")).shuffleGrouping("mentions-total-ranking");

        // Graph
        builder.setBolt("graph-connections", new GraphBolt()).fieldsGrouping("split", "links", new Fields("mention"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {
            conf.setMaxTaskParallelism(3);

            StormTopology topology = builder.createTopology();

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("word-count", conf, topology);

            builder.createTopology();

            try {
                Thread.sleep(20000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            cluster.shutdown();

            //cluster.killTopology("word-count");
        }
    }
}