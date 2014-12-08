package storm.starter;

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

    }

    @Override
    public void execute(Tuple tuple) {

        String username = tuple.getString(0);
        String mention = tuple.getString(1);

        Node a;
        Node b;

        if(!username.equals(mention))
        {
            if(!nodeExist(username)) {
                a = graph.addNode(username);
                a.addAttribute("ui.label", username);
            }
            else
            {
                a = graph.getNode(username);
            }

            if(!nodeExist(mention)) {
                b = graph.addNode(mention);
                b.addAttribute("ui.label", mention);
            }
            else
            {
                b = graph.getNode(mention);
            }

            if(!a.hasEdgeBetween(b) && !b.hasEdgeBetween(a))
            {
                graph.addEdge(username + " " + mention, a, b);
            }
        }

        //fdsafasfasf


        /*if(!edgeExist(username, mention) && !edgeExist(mention, username)) {
            graph.addEdge(username + " " + mention, username, mention);
        }*/

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
        for(Node n : graph)
        {
            if(n.getDegree() < 5)
            {
                graph.removeNode(n);
            }
        }
        graph.display();

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}