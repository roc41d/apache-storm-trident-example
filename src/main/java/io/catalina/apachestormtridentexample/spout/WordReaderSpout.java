package io.catalina.apachestormtridentexample.spout;

import io.catalina.apachestormtridentexample.util.Constants;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReaderSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;

    private FileReader fileReader;
    private BufferedReader reader;

    private boolean completed = false;

    @Override
    public void open(Map<String, Object> config, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;

        try {
            this.fileReader = new FileReader(config.get(Constants.FILE_TO_READ).toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file["+ config.get(Constants.FILE_TO_READ) +"]");
        }

        this.reader = new BufferedReader(fileReader);
    }

    @Override
    public void nextTuple() {
        if (!completed) {
            try {
                String word = reader.readLine();
                if (word != null) {
                    word = word.trim();
                    word = word.toLowerCase();

                    collector.emit(new Values(word));
                } else {
                    completed = false;
                    fileReader.close();
                }
            } catch (Exception e) {
                throw  new RuntimeException("Error reading tuple", e);
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(Constants.WORD_FIELD));
    }
}
