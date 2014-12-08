package storm.starter.util;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.Map;

import org.graphstream.graph.*;
import org.graphstream.graph.implementations.*;

// Created by Adrar on 08/12/2014

public class GraphBolt implements IRichBolt {

    private OutputCollector _collector;
    private Graph graph;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        _collector = outputCollector;
        graph = new SingleGraph("graph");
        graph.display();
    }

    @Override
    public void execute(Tuple tuple) {

        String username = (String) tuple.getValue(0);
        String mention = (String) tuple.getValue(1);

        if(!nodeExist(username)) {
            graph.addNode(username);
        }

        if(!nodeExist(mention)) {
            graph.addNode(mention);
        }

        if(!edgeExist(username, mention)) {
            graph.addEdge(username + " " + mention, username, mention);
        }
    }

    public boolean edgeExist(String a, String b) {
        for (Edge e : graph.getEachEdge()) {
            if (e.getId().equals(a + " " + b)) {
                return true;
            }
        }
        return false;
    }

    public boolean nodeExist(String s) {
        for (Node n : graph.getEachNode()) {
            if (n.getId().equals(s)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}