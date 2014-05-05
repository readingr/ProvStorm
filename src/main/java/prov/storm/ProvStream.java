package prov.storm;

import java.net.*;
import java.util.ArrayList;
import java.util.Random;
import java.io.*;

public class ProvStream {

	//static ServerSocket variable
	private static ServerSocket server;
	//socket server port on which it will listen
	private static int port = 9876;
	static ArrayList<String> action = new ArrayList<String>();

	
	public static void main(String args[]) throws IOException, ClassNotFoundException{
		//create the socket server object
		server = new ServerSocket(port);
		
		action.add("    <prov:wasDerivedFrom>\n" + 
				"        <prov:generatedEntity prov:ref=\"e2\"/>\n" + 
				"        <prov:usedEntity prov:ref=\"e1\"/>\n" + 
				"    </prov:wasDerivedFrom>");
		action.add("    <prov:wasDerivedFrom>\n" + 
				"        <prov:generatedEntity prov:ref=\"e3\"/>\n" + 
				"        <prov:usedEntity prov:ref=\"e2\"/>\n" + 
				"    </prov:wasDerivedFrom>");
		action.add("    <prov:wasDerivedFrom>\n" + 
				"        <prov:generatedEntity prov:ref=\"e4\"/>\n" + 
				"        <prov:usedEntity prov:ref=\"e3\"/>\n" + 
				"    </prov:wasDerivedFrom>");
		action.add("    <prov:wasDerivedFrom>\n" + 
				"        <prov:generatedEntity prov:ref=\"e5\"/>\n" + 
				"        <prov:usedEntity prov:ref=\"e4\"/>\n" + 
				"    </prov:wasDerivedFrom>");
		action.add("    <prov:wasDerivedFrom>\n" + 
				"        <prov:generatedEntity prov:ref=\"e6\"/>\n" + 
				"        <prov:usedEntity prov:ref=\"e1\"/>\n" + 
				"    </prov:wasDerivedFrom>");
		
		
		
		
		System.out.println("Starting Stream");
		System.out.println("e6 is the last entity Storm will find, terminate manually after that.");
		//keep listens indefinitely 
		while(true){
//			System.out.println("Waiting for client request");
					
			
			
			//creating socket and waiting for client connection
			Socket socket = server.accept();
			

			//create ObjectOutputStream object
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			
			Random rand = new Random();
			
			if (rand.nextInt(150) == 0){
				if(action.size() > 0){
					oos.writeObject(action.get(0));
					action.remove(0);
				}
				else{
					//once we've sent all the entities, we keep sending dummy data
					//this is because I don't know how to tell storm to stop, so this stops error messages.
//					System.out.println("server needs restarting to find more values.");
					oos.writeObject("<prov:wasDerivedFrom>\n" + 
							"        <prov:generatedEntity prov:ref=\"d2\"/>\n" + 
							"        <prov:usedEntity prov:ref=\"d1\"/>\n" + 
							"    </prov:wasDerivedFrom>");
				}
				
			}
			else{
				oos.writeObject("<prov:wasDerivedFrom>\n" + 
						"        <prov:generatedEntity prov:ref=\"d2\"/>\n" + 
						"        <prov:usedEntity prov:ref=\"d1\"/>\n" + 
						"    </prov:wasDerivedFrom>");
			}

			
			oos.close();
			socket.close();
			
			//terminate the server if client sends exit request
			if(action.size() == -1) break;
		}
		System.out.println("Shutting down Socket server!!");
		
		//close the ServerSocket object
		server.close();
	}

}
