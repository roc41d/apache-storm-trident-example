package io.catalina.apachestormtridentexample;

import io.catalina.apachestormtridentexample.spout.WordReaderSpout;
import io.catalina.apachestormtridentexample.util.Constants;
import io.catalina.apachestormtridentexample.util.SplitFunction;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.tuple.Fields;

public class TridentTopologyRunner {
    public static void main(String[] args) throws Exception {

        // Build Topology
        TridentTopology topology = new TridentTopology();

        topology.newStream(Constants.WORD_READER_SPOUT, new WordReaderSpout())
                .each(new Fields(Constants.WORD_FIELD), /*fields in the input tuple*/
                        new SplitFunction(), /* function/transform to be apply/perform on the stream*/
                        new Fields(Constants.SPLIT_WORD_FIELD)) /* fields in the output tuple */
                .each(new Fields(Constants.SPLIT_WORD_FIELD), new Debug()); /* prints each tuple in the input stream on the screen*/

        // Configuration
        Config config = new Config();
        config.setDebug(true);
        config.put(Constants.FILE_TO_READ, "pathToFile");

        //Submit Topology to cluster
        if (args != null && args.length > 0) {
//            config.setNumWorkers();
            // submit to remote cluster
            StormSubmitter.submitTopology(args[0], config, topology.build());
            return;
        }

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(Constants.TOPOLOGY_NAME, config, topology.build());
    }
}
