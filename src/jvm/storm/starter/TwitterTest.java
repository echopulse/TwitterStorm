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
import backtype.storm.tuple.Values;
import storm.starter.bolt.IntermediateRankingsBolt;
import storm.starter.bolt.RollingCountBolt;
import storm.starter.bolt.TotalRankingsBolt;
import storm.starter.spout.RandomSentenceSpout;
import storm.starter.spout.TwitterSampleSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by gperinazzo on 28/11/2014.
 */
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
        //Unmodified
        builder.setBolt("hashtag-counter", new RollingCountBolt(9, 3), 3).fieldsGrouping("split", "hashtag", new Fields("hashtags"));
        //Unmodified
        builder.setBolt("hashtag-intermediate-ranking", new IntermediateRankingsBolt(10), 3).fieldsGrouping("hashtag-counter", new Fields("obj"));
        //Unmodified
        builder.setBolt("hashtag-total-ranking", new TotalRankingsBolt(10)).globalGrouping("hashtag-intermediate-ranking");

        builder.setBolt("hashtag-ranking-print", new RankingPrinterBolt("HASHTAG_RANKING.txt")).shuffleGrouping("hashtag-total-ranking");


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
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            cluster.shutdown();

            //cluster.killTopology("word-count");

            cluster.shutdown();



        }
    }
}