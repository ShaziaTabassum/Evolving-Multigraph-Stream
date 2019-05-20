/* Program to forget the edges from an evolving multigraph stream or weighted temporal graph stream using a 
 * exponential smoothing factor and threshold, which generates dynamic samples at any timestep t from a network Stream. 
 * Below program saves the snapshot of the sample after every timestep.
 * Input: Many files of different time steps in order.
 * Output: The weight/frequency of edges is multiplied after a new input file with a time step change (year, month or day) 
 * with att_factor then prunned using threshold and written in an output file. 
 * The forgetting factor is computed in continuation from file 1 to n. 
 */
/* ATT_FACTOR ranges between 0 and 1 including.
 * THRESHOLD ranges between 0 and highest frequency of an edge in the network, 
 * when it is set to highest frequency all the network is forgotten. 
 * You can start experimenting from low value ranges between 0 and 1 for Threshold as well
 * and increase to decrease the sample size.
*/
package dynamic.sampling;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ExponentialSmoothing {
	private static Map<String, Double> edgeDetailsMap = new HashMap<String, Double>();
	private static Map<String, Double> currentEdgeDetailsMap;
	private static double ATT_FACTOR = 0.2;
	private static double ATT_FACTOR_FOR_CURRENTDAY = 1-ATT_FACTOR;
	private static double THRESHOLD = 12.0;
	private static int fileName=1;
	private static int lastfileName=5; 
	public static final String INPUT_FOLDER_NAME = "/home/path/";
	public static final String OUTPUT_FOLDER_NAME = "/home/path/";
	
	public static void main(String[] args) {
		try {
			long startTimeinMilliSeconds = new Date().getTime();
			
				for(; fileName<=lastfileName; fileName++){ //#of files in the folder

				
				/*NumberFormat nf = NumberFormat.getNumberInstance(Locale.UK);
				nf.setMaximumFractionDigits(2);*/
				
				currentEdgeDetailsMap = new HashMap<String, Double>();
				readFolderForGivenFiles(INPUT_FOLDER_NAME,fileName);
				
				File file = new File(OUTPUT_FOLDER_NAME+fileName);
				
				
				// if file doesnt exists, then creates it
				if (!file.exists()) {
					file.createNewFile();
				}

				FileWriter writer = new FileWriter(file, true);

				writer.append("SOURCE,TARGET,WEIGHT");
				for(Entry<String, Double> og : edgeDetailsMap.entrySet()){
					writer.append("\r\n"+og.getKey()+","+og.getValue());
				}

				writer.flush();
				writer.close();
				
							
			}
				
				long endTimeinMilliSeconds = new Date().getTime();

				System.out.println("Time to compute in MilliSeconds: "
						+ (endTimeinMilliSeconds -startTimeinMilliSeconds));
			System.out.println("DONE !");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void readFolderForGivenFiles(String folderName,
			int fileName) {
		
		// multiplies weights with att_factor after adding new day (i.e current day)
		File file = new File(folderName + "//" + fileName);
		BufferedReader br = null;
		try {

			String sCurrentLine;
			String DELIMETER = ",";

			System.out.println("Reading File : " + fileName	+ "    ..............");
			br = new BufferedReader(new FileReader(file));
			while ((sCurrentLine = br.readLine()) != null) {
				// Ignores the First Line ,as it contains the column name
				if (sCurrentLine.contains("SOURCE")) {
					continue;
				}
				String[] splitLine = sCurrentLine.split(DELIMETER);

//				String date = getDate(splitLine[5].trim());
				String source = splitLine[0].trim();
				String target = splitLine[1].trim();

				String key = source + "," + target;
				String keyflip = target + "," + source;
				
				
			
				if (currentEdgeDetailsMap.containsKey(key)||currentEdgeDetailsMap.containsKey(keyflip)) 
				{
					if (currentEdgeDetailsMap.containsKey(key)){
						currentEdgeDetailsMap.put(key, currentEdgeDetailsMap.get(key) + 1);
					}
					else
						currentEdgeDetailsMap.put(keyflip, currentEdgeDetailsMap.get(keyflip) + 1);
					
				} else {
					currentEdgeDetailsMap.put(key, 1.0);
				}

			}
			
			for (Entry<String, Double> og : currentEdgeDetailsMap.entrySet()) 
				{
				currentEdgeDetailsMap.put(og.getKey(), og.getValue() * (ATT_FACTOR_FOR_CURRENTDAY));
				}
			
			for (Entry<String, Double> og : edgeDetailsMap.entrySet())
				{
					edgeDetailsMap.put(og.getKey(), og.getValue() * ATT_FACTOR);
				}
				
			
			for(String edge : currentEdgeDetailsMap.keySet())
			{
				String[] splitLine = edge.split(DELIMETER);

				String fromNum = splitLine[0].trim();
				String toNum = splitLine[1].trim();

				String key = fromNum + "," + toNum;
				String keyflip = toNum + "," + fromNum;	
			if (edgeDetailsMap.containsKey(key)||edgeDetailsMap.containsKey(keyflip)) 
			{
				if (edgeDetailsMap.containsKey(key)){
					edgeDetailsMap.put(key, (edgeDetailsMap.get(key) + currentEdgeDetailsMap.get(key)));
				}
				else
					edgeDetailsMap.put(keyflip, (edgeDetailsMap.get(keyflip) + currentEdgeDetailsMap.get(key)));
				
			} else {
				edgeDetailsMap.put(key, currentEdgeDetailsMap.get(key));
			}
			}

			Set<String>  toRmoveSet =  new HashSet<String>();
			for (Entry<String, Double> og : edgeDetailsMap.entrySet()) 
			{
					
				if(edgeDetailsMap.get(og.getKey()) <= THRESHOLD)
				{
							toRmoveSet.add(og.getKey());
			}
		}
			// removes the edges before the new weights are added
			for(String key : toRmoveSet)
			    edgeDetailsMap.remove(key);
	  
				
				

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	

}

