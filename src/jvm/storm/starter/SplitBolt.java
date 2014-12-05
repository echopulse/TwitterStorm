package storm.starter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by umarq on 04/12/2014.
 */
public class SplitBolt implements IRichBolt{

    private OutputCollector _collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        Object temp = tuple.getValue(0);
        Map data = (Map) temp;

        //Example Tweets
        //[RT @toppcrab: sparkling masterpiece @ToppDoggHouse #ToppKlass1stAnnie http://t.co/nj3qQzFFXr]
        //[GAK ADA UANG BISA DP BRO JAKET FAVORIT ANDA @wildan_kk CP. 089693622357/PIN. 2636AE00 http://t.co/3hVWvuLAO2]

        String str = (String) data.get("text");
        String username = (String) data.get("username");
        Date timestamp = (Date) data.get("timestamp");
        //long retweetCount = (long) data.get("retweetCount");


        String[] words = str.split("//t");

        ArrayList<String> hashtags = new ArrayList<String>();
        ArrayList<String> mentions = new ArrayList<String>();

        for(String word : words)
        {
            if(word.startsWith("#")){
                hashtags.add(word.substring(1, word.length()));
            }
            if(word.startsWith("@")){
                mentions.add(word.substring(1, word.length()));
            }


        }

        System.out.println("HHHHHHH:" + username);

        _collector.emit("user", new Values(username, timestamp));
        _collector.emit("hashtag", new Values(hashtags, timestamp));
        _collector.emit("mention", new Values(mentions, timestamp));
    }

    @Override
    public void cleanup() {

    }

    @Override
    //OUTPUT
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("user", new Fields("username", "timestamp"));
        declarer.declareStream("hashtag", new Fields("hashtags", "timestamp"));
        declarer.declareStream("mention", new Fields("mention", "timestamp"));

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
