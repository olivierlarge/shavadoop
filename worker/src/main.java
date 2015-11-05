import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;


public class main {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		String mode = args[0];
		String data = "";
		String UMx = "";
		String SMx = "";
		String key = "";
		List<String> umCible = new ArrayList<String>();
	   	if(mode.equals("modeSxUMx")){
	   		UMx = args[1];
	   		data = args[2];
	   		modeSxUMx(UMx, data);
	   	} else if(mode.equals("modeUMxSMx")){
	   		key = args[1];
	   		SMx = args[2];
	   		for(int i = 3; i<args.length; i++){
	   			umCible.add(args[i]);
	   		}
	   		modeUMxSMx(key, SMx, umCible);
	   	}	
	}

	private static void modeSxUMx(String UMx, String data) throws FileNotFoundException, UnsupportedEncodingException {
		String dicosend = "";
		//le nom du fichier est dicosend + timestamp car une machine peut lancer plusieurs map
		String newline = null;
		HashMap<String, Integer> map = new HashMap<>();
		dicosend = UMx;
		PrintWriter writer = new PrintWriter("/cal/homes/olarge/shavadoop/datamapper/" + UMx + ".txt", "UTF-8");
		for(String el : data.split(" ")){
		   if(!map.containsKey(el)){
			   map.put(el, 1);
			   dicosend += " " + el;
		   }
		writer.write(el + " 1\n");
		}
		writer.close();
		System.out.println(dicosend);
       //System.out.println(map);
	}
	
	private static void modeUMxSMx(String key, String SMx, List<String> umCible) throws IOException {
			//Read each umCible
			ArrayList<String> arrayVal = new ArrayList<String>();
			for (String umFile : umCible){
				
				BufferedReader br = null;
				String sCurrentLine;
				//System.out.println(umFile);
				FileReader in = new FileReader("/cal/homes/olarge/shavadoop/datamapper/" + umFile + ".txt");
				br = new BufferedReader(in);
				while ((sCurrentLine = br.readLine()) != null) {
					if(sCurrentLine.split(" ")[0].equals(key)){
						arrayVal.add(sCurrentLine.split(" ")[1]);
					}
				}
				in.close();
			}
			//Shuffle
			PrintWriter writer = new PrintWriter("/cal/homes/olarge/shavadoop/datamapper/" + SMx + ".txt", "UTF-8");	
			for(String val : arrayVal){
				writer.write(key + " " + val + "\n");
			}
			writer.close();
			String RMx = "RM" + SMx.split("M")[1];
			//Reduce
			BufferedReader br = null;
			String sCurrentLine;
			int output = 0;
			br = new BufferedReader(new FileReader("/cal/homes/olarge/shavadoop/datamapper/" + SMx + ".txt"));
			PrintWriter writerRMx = new PrintWriter("/cal/homes/olarge/shavadoop/datamapper/" + RMx + ".txt", "UTF-8");	
			while ((sCurrentLine = br.readLine()) != null) {
					output++;
			} 
			writerRMx.write(key + " " + output + "\n");
			writerRMx.close();
			
			System.out.println(Inet4Address.getLocalHost().getHostAddress() + " " + RMx);
	}
}
