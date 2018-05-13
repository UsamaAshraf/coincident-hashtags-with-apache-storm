package coincident_hashtags;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.testing.TestWordSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;


public class ExclamationTopology {

  public static class ExclamationBolt extends BaseRichBolt
  {
    OutputCollector _collector;

    @Override
    public void prepare(
        Map                     map,
        TopologyContext         topologyContext,
        OutputCollector         collector)
    {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
      // get the column word from tuple
      String word = tuple.getString(0);

      // build the word with the exclamation marks appended
      StringBuilder exclamatedWord = new StringBuilder();
      exclamatedWord.append(word).append("!!!");

      // emit the word with exclamations
      _collector.emit(tuple, new Values(exclamatedWord.toString()));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
      // Tell storm the schema of the output tuple for this spout.
      // The tuple consists of a single column called 'exclamated-word'
      declarer.declare(new Fields("exclamated-word"));
    }
  }

  public static void main(String[] args) throws Exception
  {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("word", new TestWordSpout(), 10);

    builder.setBolt("exclaim1", new ExclamationBolt(), 3).shuffleGrouping("word");

    builder.setBolt("exclaim2", new ExclamationBolt(), 2).shuffleGrouping("exclaim1");

    // Create a config object.
    Config conf = new Config();

    // Turn on debugging mode.
    conf.setDebug(true);
    
    // Set the number of workers for running all spout and bolt tasks.
    // If we have two supervisors with 4 allocated workers each, and this topology is
    // submitted to the master (Nimbus) node, then these 8 workers will be distributed
    // among the two supervisors evenly: four each. 
    conf.setNumWorkers(3);

    StormSubmitter.submitTopology("exclamation-topology", conf, builder.createTopology());
  }
}
