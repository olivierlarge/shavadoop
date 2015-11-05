import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.AllPermission;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;


public class mainMaster {
	public static ArrayList<String> ipreachables = new ArrayList<>();
	public static ArrayList<String> ipvalides = new ArrayList<>();
	public static HashMap<String, String> dicoUmxMachine = new HashMap<>();
	public static HashMap<String, ArrayList<String>> dicoCleUmx = new HashMap<String, ArrayList<String>>();
	public static ArrayList<String> arrayDicoCleUmx = new ArrayList<String>();
	public static HashMap<String, String> dicoRmxMachine = new HashMap<>();
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
        System.out.println("Début du programme local");
        
        //Récupérer la liste des hotes reachables du 0
        
        int timeout=10;
        //for (int i=30;i<36;i++){
        for (int j=1;j<25;j++){
            String host="137.194" + "." + "34" + "." + j;
            try {
				if (InetAddress.getByName(host).isReachable(timeout)){
				    //System.out.println(host + " is reachable");
				    ipreachables.add(host);
				}
			} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        //}
        System.out.println(ipreachables.size() + " machines pingables");
        //Récupérer liste des hotes actifs
        HashMap<Long, AfficheurFlux> threadsAndRuns = new HashMap<Long, AfficheurFlux>();
        ArrayList<Thread> allThreads = new ArrayList<Thread>();
        for (String ip_test : ipreachables){
	        try{
	        	//System.out.println("ip send : " + ip_test);
	        	String[] commandetest = {"ssh",ip_test, "echo a " + ip_test};
	        	Process p = new ProcessBuilder(commandetest).start();
	            AfficheurFlux fluxSortie = new AfficheurFlux(p.getInputStream());
	            Thread a = new Thread(fluxSortie);
	            a.start();
	            allThreads.add(a);
	            threadsAndRuns.put(a.getId(), fluxSortie);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
        }
        AfficheurFlux fluxRecup;
        for(Thread el : allThreads){
        	el.join();
        	System.out.print("=");
        }
        for(Thread el : allThreads){
        	//recup le flux sortie de el
        	fluxRecup = threadsAndRuns.get(el.getId());
        	
        	if(fluxRecup.getLignefin() != null){
        	if(fluxRecup.getLignefin().matches("a.*")){
            	ipvalides.add(fluxRecup.getLignefin().split(" ")[1]);
            }}
        }
        System.out.println("\n" + ipvalides.size() + " machines disponibles");
        //Input fichier
       ByteArrayOutputStream baos = new ByteArrayOutputStream();
       System.setOut(new PrintStream(baos));
        map("/cal/homes/olarge/shavadoop/fichiertestMR");
        //map("/cal/homes/olarge/legifrance/Les-codes-en-vigueur/aviation_civile.txt");
//        try {
//			Thread.sleep(10000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
        System.setOut(new PrintStream(new FileOutputStream(FileDescriptor.out)));
        System.out.println("dictionnaire UMx - Machine : " + dicoUmxMachine);
        
        for(String line : baos.toString().split("\n")){
        	System.out.println(line);
        	String valDicoCleUmx = line.split(" ")[0];
        	for(int i=1;i<line.split(" ").length;i++){
        		String keyDicoCleUmx = line.split(" ")[i];
        		if(!dicoCleUmx.containsKey(keyDicoCleUmx)){
        			arrayDicoCleUmx = new ArrayList<String>();       			
        		} else{
        			arrayDicoCleUmx = dicoCleUmx.get(keyDicoCleUmx);
        		}
        		arrayDicoCleUmx.add(valDicoCleUmx);
        		dicoCleUmx.put(keyDicoCleUmx, arrayDicoCleUmx);
        	}
        }
        System.out.println("dictionnaire clé - UMx : " + dicoCleUmx);
        
        shuffle();
        reduce();
        System.out.println("Fin du programme local");
    }
	
	
	
    private static BufferedReader getBufferedReader(InputStream is) {
        return new BufferedReader(new InputStreamReader(is));
    }



	public static int map(String fileName) {
	    try {
	        Scanner e = new Scanner(new FileReader(fileName));
	        String ligne = null;
	        String UMx = "";
	        int cnt_ipvalide = 0;
	        int lineNumber = 1;
	        HashMap<Thread, AfficheurFlux> threadsAndRunsMap = new HashMap<Thread, AfficheurFlux>();
	        ArrayList<Thread> mapThreads = new ArrayList<Thread>();
	        //For each line in file, we launch a command on a machine. 
	        while(e.hasNextLine()) {
	        	UMx = "UM" + lineNumber;
	            ligne = e.nextLine();
	            try {
	            	//Arguments for mode modeSxUMx are "modeSxUMx UMx data"
	                String[] commande = {"ssh",ipvalides.get(cnt_ipvalide), "java -jar Exec.jar modeSxUMx "+ UMx +" \"" + ligne + "\""};
	                Process p = new ProcessBuilder(commande).start();
	                AfficheurFlux fluxSortie = new AfficheurFlux(p.getInputStream());
	                Thread newThread = new Thread(fluxSortie);
	                newThread.start();
	                mapThreads.add(newThread);
	                threadsAndRunsMap.put(newThread, fluxSortie);
	            } catch (IOException e1) {
	                e1.printStackTrace();
	            }
	            //we add the line to our dicoUmxMachine where key is the number of line 
	            dicoUmxMachine.put(UMx, ipvalides.get(cnt_ipvalide));
	            cnt_ipvalide++;
	            lineNumber++;
	            //if there is no machine anymore, we send another command to the firsts machine.
	            if(cnt_ipvalide>=ipvalides.size()){
	            	cnt_ipvalide=0;
	            }
	        }
	        for(Thread th : mapThreads){
	        	th.join();
	        	System.out.println(threadsAndRunsMap.get(th).getLignefin());
	        }

	    } catch (Exception a) {
	        a.printStackTrace();
	    }

	    return 1;
	}
	
	public static int shuffle() throws InterruptedException{
		//For each key in dicoCleUmx, we send to the first machine associated
		int smNumber = 0;
		HashMap<Long, AfficheurFlux> threadsAndRunsShuf = new HashMap<Long, AfficheurFlux>();
		HashMap<String, Integer> machineLoad = new HashMap<>();
		float globalCharge = 0F;
		for(String el : ipvalides) {
			machineLoad.put(el, 0); //On initialise toutes les machines à la charge 0
		}
		
		ArrayList<Thread> allThreadsShuf = new ArrayList<Thread>();
		for(String key : dicoCleUmx.keySet()){
			String SMx = "SM" + smNumber;
			int i =0;
			//we get first UMX to retrieve machine : TODO load function metric to optimize machineCible to send
			String machineCible = dicoUmxMachine.get(dicoCleUmx.get(key).get(0));
			while(machineLoad.get(machineCible)>3){
				machineCible = ipvalides.get(i);
				if(i<ipvalides.size()-1){
					i++;
				} else{
					long startTime = System.currentTimeMillis(); //fetch starting time
					while((System.currentTimeMillis()-startTime)<5000)
					{
						System.out.println("==== UNLOAD CHARGE ON ====");
						//On décharge des threads qui ont peut être terminé
						AfficheurFlux fluxRecup = null;
						for(Thread el : allThreadsShuf){
							fluxRecup = threadsAndRunsShuf.get(el.getId());
							if(fluxRecup.getLignefin()!=null){
				        	String machineUnLoad = fluxRecup.getLignefin().split(" ")[0];
				        	machineLoad.put(machineUnLoad, machineLoad.get(machineUnLoad)-1);
				        	globalCharge--;
							}
						}
					}
				}
			}
			//Fonction pour tester un déchargement si on est à 70% de charge
			if(globalCharge/ipvalides.size()>0.7){
				long startTime = System.currentTimeMillis(); //fetch starting time
				while((System.currentTimeMillis()-startTime)<5000)
				{
					System.out.println("==== UNLOAD CHARGE ON ====");
					//On décharge des threads qui ont peut être terminé
					AfficheurFlux fluxRecup = null;
					for(Thread el : allThreadsShuf){
						fluxRecup = threadsAndRunsShuf.get(el.getId());
						if(fluxRecup.getLignefin()!=null){
			        	String machineUnLoad = fluxRecup.getLignefin().split(" ")[0];
			        	machineLoad.put(machineUnLoad, machineLoad.get(machineUnLoad)-1);
			        	globalCharge--;
						}
					}
				}
			}
			machineLoad.put(machineCible, machineLoad.get(machineCible)+1);
			String allUMX = "";
			for(String UMx : dicoCleUmx.get(key)){
				allUMX += UMx + " ";
			}
			System.out.println(key + " " + SMx + " " + allUMX);
			String[] commandeShuf = {"ssh",machineCible, "java -jar Exec.jar modeUMxSMx \"" + key + "\" " + SMx + " " + allUMX};
			try{
	        	Process p = new ProcessBuilder(commandeShuf).start();
	            AfficheurFlux fluxSortie = new AfficheurFlux(p.getInputStream());
	            Thread a = new Thread(fluxSortie);
	            a.start();
	            allThreadsShuf.add(a);
	            threadsAndRunsShuf.put(a.getId(), fluxSortie);
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
			dicoRmxMachine.put("RM" + smNumber, machineCible);
			smNumber++;
		}
		
		for(Thread elem : allThreadsShuf){
			elem.join();			
		}
		return 1;
	}
	public static int reduce() throws IOException{
		PrintWriter writerReduce = new PrintWriter("/cal/homes/olarge/shavadoop/datamapper/output.txt", "UTF-8");
		for(Entry<String, String> entry : dicoRmxMachine.entrySet()) {
			
			BufferedReader br = null;
			String sCurrentLine;
			br = new BufferedReader(new FileReader("/cal/homes/olarge/shavadoop/datamapper/" + entry.getKey() + ".txt"));
			
			while ((sCurrentLine = br.readLine()) != null) {
				writerReduce.write(sCurrentLine + "\n");
			} 
		}
		writerReduce.close();
		return 1;
	}
	
}

