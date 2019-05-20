/*Undirected evolving multi-graph stream random sampling. For every edge from a temporal stream, 
if the edge is already present in the reservoir its weight is increased else it replaces another edge at random. 
If an edge is being selected to be deleted then the edge and also its weight is deleted.
Input: One or many csv files with edges in the order of time.
Output: Sample snapshot.
*/
package dynamic.sampling;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

public class RandomStreamSampling {

	private static int reservoir_size = 100;
	private static int fileName=1;
	private static int lastfileName=100; 
	private static Map<Integer,String> indexMap= new HashMap<Integer,String>();;
	private static Map<String,Integer> edgeMap= new HashMap<String,Integer>();;
	private static int index=0;
	
	public static final String INPUT_FOLDER_NAME = "/home/input/";
	public static final String OUTPUT_FOLDER_NAME = "/home/output/";
	
	public static void main(String[] args) {
		
		try {
			
			long startTimeinMilliSeconds = new Date().getTime();

											
			for(; fileName<=lastfileName; fileName++){ //#of files in the folder
				
				readFolderForGivenFiles(fileName);
				
				File file = new File(OUTPUT_FOLDER_NAME +"//" +  fileName);
				
				// if file doesnt exists, then creates it
				if (!file.exists()) {
					file.createNewFile();
				

				FileWriter writer = new FileWriter(file, true);

				writer.append("SOURCE,TARGET,WEIGHT");
				for(Entry<String, Integer> og : edgeMap.entrySet()){
					writer.append("\r\n"+og.getKey()+","+og.getValue());
				}

				writer.flush();
				writer.close();
				}
				
			}
				long endTimeinMilliSeconds = new Date().getTime();

				System.out.println("Time to compute in MilliSeconds: "+(endTimeinMilliSeconds - startTimeinMilliSeconds));
								
				
				System.out.println("DONE !");
				

		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	private static int getRandomNumber(int lower, int reservoir_size2) {
		Random r = new Random();
		return r.nextInt(reservoir_size2 - lower) + lower;
	}

	public static void readFolderForGivenFiles(int fileName) {

		File file = new File(INPUT_FOLDER_NAME + "//" + fileName);

		BufferedReader br = null;

		try {

			String sCurrentLine;
			String DELIMETER = ",";

			System.out.println("Reading File:" + fileName
					+ "    ..............");

			br = new BufferedReader(new FileReader(file));

			while ((sCurrentLine = br.readLine()) != null) {

				// Ignores the First Line ,as it contains the column name
				if (sCurrentLine.contains("SOURCE")) {
					continue;
				}
				
				String[] splitLine = sCurrentLine.split(DELIMETER);

				String source = splitLine[0]
						.trim();
				String target = splitLine[1].trim();

				
				if (index < reservoir_size) {
					if(edgeMap.containsKey(source + "," + target))
					{
						int weight=edgeMap.get(source + "," + target);
						edgeMap.put(source + "," +target, weight+1);
					}
					else if(edgeMap.containsKey(target + "," + source))
					{
						int weight=edgeMap.get(target + "," + source);
						edgeMap.put(target + "," +source, weight+1);
					}
					else
					{
						edgeMap.put(source + "," + target,1);
						indexMap.put(index,source + "," + target);
						index++;
					} 
				}
				else {
					
					int pos = getRandomNumber(0, reservoir_size);
						if(edgeMap.containsKey(source + "," + target))
						{
							int weight=edgeMap.get(source + "," + target);
							edgeMap.put(source + "," + target, weight+1);
							
						}
						else if(edgeMap.containsKey(target + "," + source))
						{
							int weight=edgeMap.get(target + "," + source);
							edgeMap.put(target + "," + source, weight+1);
							
						}
						else
						{
							
							String keyToRemove=indexMap.get(pos);
							indexMap.put(pos, source + "," + target);
							edgeMap.remove(keyToRemove);
							edgeMap.put(source + "," + target, 1);
						} 
				}
				
			}

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
