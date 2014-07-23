import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.aphyr.riemann.client.RiemannClient;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.security.jca.GetInstance;

import java.io.IOException;
import java.util.Map;

public class MonitoredTopology {

    public static class MonitoredSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private int lastId = 0; //TODO: WHEN THERE WILL BE MORE THAN ONE CONCURRENT SPOUT, CHANGE THIS TO A STATIC VARIABLE
        private final Map<Integer,Long> startTimestampPerId = Maps.newHashMap();
        private String riemannIP;
        private RiemannClient client;
        private final Logger logger = LoggerFactory.getLogger(this.getClass());


        private void sendRiemannLatency(long latency, Exception ex) throws IOException {
            client.event().service("latency measuring storm").state(ex==null ? "success" : "failure").metric(latency).tags("latency").send();
        }


        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            logger.debug("HELLO DEBUG");
            logger.info("HELLO INFO");
            RiemannDiscovery discover = new RiemannDiscovery();
            String machinePrefix = null;
            try {
                machinePrefix = ( discover.retrieveName().startsWith("prod") ? "prod-" : "develop-");
                riemannIP = Iterables.get(discover.describeInstancesByName(machinePrefix+"riemann-instance"), 0).getPrivateIpAddress();
                client = RiemannClient.tcp(riemannIP, 5555);
                client.connect();
            } catch (IOException e) {
                e.printStackTrace();
            }
            this.collector = collector;
        }

        @Override
        public void close() {
            try {
                client.disconnect();
            } catch (IOException e) {
                    e.printStackTrace();
            }
            super.close();
        }

        @Override
        public void nextTuple() {
            this.collector.emit(new Values(""), lastId);
            this.startTimestampPerId.put(lastId, System.nanoTime());
            lastId++;
        }

        @Override
        public void ack(Object id) {
            long elapsed = (System.nanoTime() - startTimestampPerId.get(id)) / 1000000;
            try {
                sendRiemannLatency(elapsed, null);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void fail(Object id) {
            long elapsed = (System.nanoTime() - startTimestampPerId.get(id)) / 1000000;
            try {
                sendRiemannLatency(elapsed, new RuntimeException("Storm sent fail. No stack trace."));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class NopBolt extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            try {
                Thread.sleep(1000);
            }
            catch(Exception e) {

            }

        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new MonitoredSpout(), 1);
        builder.setBolt("bolt", new NopBolt(), 1).localOrShuffleGrouping("spout");

        Config conf = new Config();
        conf.setDebug(false);
        conf.setMaxSpoutPending(1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());

        Thread.sleep(60*1000);
        cluster.shutdown();
    }
}
