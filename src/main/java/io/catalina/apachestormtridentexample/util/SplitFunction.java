package io.catalina.apachestormtridentexample.util;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * use to apply processing logic on the data stream
 */
public class SplitFunction extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String sentence = tridentTuple.getString(0);
        String[] words = sentence.split(" ");
        for(String word : words) {
            word = word.trim();
            if (!word.isEmpty()) {
                tridentCollector.emit(new Values(word));
            }
        }
    }
}
