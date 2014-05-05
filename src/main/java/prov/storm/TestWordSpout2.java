/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package prov.storm;

import backtype.storm.Config;
import backtype.storm.topology.OutputFieldsDeclarer;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestWordSpout2 extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(TestWordSpout2.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;

    public TestWordSpout2() {
        this(true);
    }

    public TestWordSpout2(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
    }
    
    public void close() {
        
    }
        
    public void nextTuple() {
        Utils.sleep(100);
//        final String[] words = new String[] {"Richard", "Nick", "Zara", "Tom", "Ben"};
//        final Random rand = new Random();
//        final String word = words[rand.nextInt(words.length)];
  
//        String[][] test = new String[][] {
//        		["entity"]
//        };
        
        ArrayList<String[]> action = new ArrayList<String[]>();
        action.add(new String[]{"entity","e1", null});
        action.add(new String[]{"entity", "e2", null});
        action.add(new String[]{"wasDerivedFrom", "e2", "e1"});
        action.add(new String[]{"wasDerivedFrom", "e3", "e2"});
        action.add(new String[]{"wasDerivedFrom", "e4", "e3"});
        action.add(new String[]{"wasDerivedFrom", "e5", "e1"});

        
        for (int i = 0; i < action.size(); i++) {
        	String[] test = action.get(i);
            _collector.emit(new Values(test[0], test[1], test[2]));
		}
    }
    
    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("type", "fieldOne", "fieldTwo"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }    
}