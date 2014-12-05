package storm.starter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import storm.starter.tools.Rankable;
import storm.starter.tools.Rankings;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

/**
 * Created by gperinazzo on 05/12/2014.
 */
public class RankingPrinterBolt extends BaseRichBolt {

    PrintWriter writer;
    int count = 0;
    private OutputCollector _collector;
    private String filename;

    public RankingPrinterBolt(String filename) {
        this.filename = filename;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        try {
            writer = new PrintWriter(filename, "UTF-8");
        } catch (FileNotFoundException e) {
            e.printStackTrace(); //To change body of catch statement use File | Settings | File Templates.
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace(); //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Override
    public void execute(Tuple tuple) {
        Object temp = tuple.getValue(0);
        Rankings ranks = (Rankings) tuple.getValue(0);
        writer.println("Rankings - ");
        List<Rankable> list;
        list = ranks.getRankings();
        for (Rankable r : list)
            writer.println((String)r.getObject() + " : " + r.getCount());
        writer.flush();
        // Confirm that this tuple has been treated.
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    }

    @Override
    public void cleanup() {
        writer.close();
        super.cleanup();
    }
}