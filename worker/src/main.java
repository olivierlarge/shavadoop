import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
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
	 * @throws UnsupportedEncodingException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException{
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
	   		int i = 3;
	   		while(args[i]!=null){
	   			umCible.add(args[i]);
	   			i++;
	   		}
	   		
	   	}	
	}

	private static void modeSxUMx(String UMx, String data) throws FileNotFoundException, UnsupportedEncodingException {
		String dicosend = "";
		//le nom du fichier est dicosend + timestamp car une machine peut lancer plusieurs map
		PrintWriter writer = new PrintWriter("/cal/homes/olarge/shavadoop/datamapper/" + UMx + ".txt", "UTF-8");
		String newline = null;
		HashMap<String, Integer> map = new HashMap<>();
		dicosend = UMx;
		for(String el : data.split(" ")){
		   if(!map.containsKey(el)){
			   map.put(el, 1);
			   dicosend += " " + el;
		   }
		writer.println(el + " 1");
		}
		System.out.println(dicosend);
       //System.out.println(map);
	}
	
	private static void modeUMxSMx(String key, String SMx, List<String> umCible) throws FileNotFoundException, UnsupportedEncodingException {
		for(String UMx : umCible){
			
		}
	}
}
