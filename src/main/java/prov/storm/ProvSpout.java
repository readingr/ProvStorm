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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;


public class ProvSpout extends BaseRichSpout {
	public static Logger LOG = LoggerFactory.getLogger(ProvSpout.class);
	boolean _isDistributed;
	SpoutOutputCollector _collector;
	int count;

	public ProvSpout() {
		this(true);
	}

	public ProvSpout(boolean isDistributed) {
		_isDistributed = isDistributed;
	}

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		_collector = collector;
		count = 0;

	}

	public void close() {

	}

	public void nextTuple() {
		//		Utils.sleep(100);


		InetAddress host;
		try {
			host = InetAddress.getLocalHost();
			Socket socket = null;
			ObjectOutputStream oos = null;
			ObjectInputStream ois = null;
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			DocumentBuilder builder = factory.newDocumentBuilder();	    

	        socket = new Socket(host.getHostName(), 9876);
	        ois = new ObjectInputStream(socket.getInputStream());
	        
	        //get from the XML stream
	        String message = (String) ois.readObject();
	        
	        Document doc = builder.parse( new InputSource( new StringReader( message ) ) ); 

            NodeList nList = doc.getElementsByTagName("prov:wasDerivedFrom");
            
            //take the derivations, and send them to the bolt
            for (int i = 0; i < nList.getLength(); i++) {
    			Node nNode = nList.item(i);
    			
    			//cast to element so we can traverse.
    			Element docElement = (Element)nNode;
    			
    			//get the generated and used entities, and send them to the bolt
    			String generatedEntity = docElement.getElementsByTagName("prov:generatedEntity").item(0).getAttributes().getNamedItem("prov:ref").getTextContent();			
    			String usedEntity = docElement.getElementsByTagName("prov:usedEntity").item(0).getAttributes().getNamedItem("prov:ref").getTextContent();			
    			_collector.emit(new Values("wasDerivedFrom", generatedEntity, usedEntity));

            }
            
            
            ois.close();
	        
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (ParserConfigurationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SAXException e) {
			e.printStackTrace();
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