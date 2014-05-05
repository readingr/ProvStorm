package prov.storm;

import java.net.*;
import java.util.ArrayList;
import java.io.*;

public class ProvStream {

	//static ServerSocket variable
	private static ServerSocket server;
	//socket server port on which it will listen
	private static int port = 9876;
	static ArrayList<String[]> action = new ArrayList<String[]>();

	
	public static void main(String args[]) throws IOException, ClassNotFoundException{
		//create the socket server object
		server = new ServerSocket(port);
		action.add(new String[]{"entity","e1", null});
		action.add(new String[]{"entity", "e2", null});
		action.add(new String[]{"wasDerivedFrom", "e2", "e1"});
		action.add(new String[]{"wasDerivedFrom", "e3", "e2"});
		action.add(new String[]{"wasDerivedFrom", "e4", "e3"});
		action.add(new String[]{"wasDerivedFrom", "e5", "e1"});
		
		//keep listens indefinitely until receives 'exit' call or program terminates
		while(true){
			System.out.println("Waiting for client request");
			
			String t = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" + 
					"<prov:document xmlns:prov=\"http://www.w3.org/ns/prov#\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns=\"http://example.org\" xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\">\n" + 
					"    <prov:entity prov:id=\"e1\"/>\n" + 
					"    <prov:entity prov:id=\"e2\"/>\n" + 
					"    <prov:wasDerivedFrom>\n" + 
					"        <prov:generatedEntity prov:ref=\"e2\"/>\n" + 
					"        <prov:usedEntity prov:ref=\"e1\"/>\n" + 
					"    </prov:wasDerivedFrom>\n" + 
					"</prov:document>\n" + 
					"";
			
			
			
			//creating socket and waiting for client connection
			Socket socket = server.accept();
			

			//create ObjectOutputStream object
			ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
			
			oos.writeObject("<prov:entity prov:id=\"e1\"/>");
			
			
			oos.close();
			socket.close();
			
			//terminate the server if client sends exit request
			if(action.size() == 0) break;
		}
		System.out.println("Shutting down Socket server!!");
		
		//close the ServerSocket object
		server.close();
	}

}
