package prov.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.Map;

/**
 * This is a basic example of a Storm topology.
 */
public class ProvTopology {

	public static class ProvBolt extends BaseRichBolt {
		OutputCollector _collector;
		
		String initialValue = "e1";
		
		
		ArrayList<String> valuesToSearchFor;
		int count;

		@Override
		public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
			_collector = collector;
			valuesToSearchFor = new ArrayList<String>();
			valuesToSearchFor.add(initialValue);



		}

		@Override
		public void execute(Tuple tuple) {



			//we check for the value we need.	
			if (tuple.getString(0).equals("wasDerivedFrom")){
				if(valuesToSearchFor.contains(tuple.getString(2))){
					if(!valuesToSearchFor.contains(tuple.getString(1))){
						System.out.println("Found "+tuple.getString(1)+ " added to the list of values to search for"); 
						valuesToSearchFor.add(tuple.getString(1));
					}

				}
			}
			
			//this below emits the tuple onto a new stream, after appending three ! marks.
			//this could be used to send the correct tuples to a new bolt, which could then email users.
			//      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!" ));
			//      _collector.ack(tuple);


		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}


	}



	public static void main(String[] args) throws Exception {


		TopologyBuilder builder = new TopologyBuilder();

		//get the prov tuples from here
		builder.setSpout("getProv", new ProvSpout(), 1);

		//pass them into the bolts, which will notify the user if they are found.
		builder.setBolt("parseProv", new ProvBolt(), 1).shuffleGrouping("getProv");

		Config conf = new Config();

		//false makes it easier to see output.
		//set true if you want to see the executor and task output.
		conf.setDebug(false);

		//if it's not run locally.
		if (args != null && args.length > 0) {
			conf.setNumWorkers(1);

			StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
		}
		else {

			System.out.println("Starting");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("test", conf, builder.createTopology());
			//      Utils.sleep(10000);
			//      cluster.killTopology("test");
			//      cluster.shutdown();
		}
	}
}
