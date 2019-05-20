/* Program to forget the edges from an evolving multigraph stream using a forgetting factor and threshold.
 * This generates dynamic samples (at any timestep t) of network which are biased to more active and latest edges
 * in the dynamic network Stream. Below program saves the snapshot of sample after every timestep.
 * Input: Many files of different time steps or one file with time stamps
 * Output: The weight/frequency of edges is multiplied after each time stamp change (year, month or day) with attfactor and threshold 
 * or can be multiplied for a new input file (if each file is represents one timestep) and written in an output file. 
 * The forgetting factor is computed in continuation from file 1 to n or all timesteps, though outputs are written after each input file.
*/
/* ATT_FACTOR ranges between 0 and 1 including. When it is set to 1.0 and ThRESHOLD is set to 0.0, it saves the true network without sampling.
 * THRESHOLD ranges between 0 and highest frequency of an edge in the network, when it is set to highest frequency all the network is forgotten. 
 * You can start experimenting from low value ranges between 0 and 1 for Threshold as well and increase to decrease the sample size.
*/
/* Algorithm of SBias and datasets are described in Tabassum, S., & Gama, J. (2018, December). Biased Dynamic Sampling for Temporal Network Streams. 
 * In International Conference on Complex Networks and their Applications (pp. 512-523). Springer, Cham.
*/

package dynamic.sampling;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class SBias {
	private static Map<String, EdgeDataWeight> edgeMap = new HashMap<String, EdgeDataWeight>();
	private static String currentDate = null;
	private static double ATT_FACTOR = 1.0;
	private static double THRESHOLD = 0.0;
	private static int start_file_num = 1;
    private static int last_file_num = 100;

	public static void main(String[] args) {
		try {
				long startTimeinMilliSeconds = new Date().getTime();
			
				for(int fileName=start_file_num; fileName<=last_file_num; fileName++){ 
								
				readPhoneCallsFolderForGivenFiles("/folder/path",fileName);			
				
				File file = new File("/result/folder/path/"+fileName);
				
				
				// if file doesnt exists, then creates it
				if (!file.exists()) {
					file.createNewFile();
				}

				FileWriter writer = new FileWriter(file, true);

				writer.append("SOURCE,TARGET,WEIGHT");
				for(Entry<String, EdgeDataWeight> og : edgeMap.entrySet()){
					writer.append("\r\n"+og.getKey()+","+og.getValue().getWeight());
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

	public static void readPhoneCallsFolderForGivenFiles(String folderName,
			Integer fileName) {
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

				String date = getDate(splitLine[2].trim());
				String source = splitLine[0].trim();
				String target = splitLine[1].trim();

				String key = source + "," + target;
				
				// check for new time stamp to forget old data
				//forgets as soon as enters new time step
				if (currentDate != null && !date.equals(currentDate)) {
					Set<String>  toRmoveSet =  new HashSet<String>();
					
					for (Entry<String, EdgeDataWeight> og : edgeMap.entrySet()) {
						EdgeDataWeight cdw = og.getValue();
						cdw.setWeight(cdw.getWeight() * ATT_FACTOR);
						
						/*remove the edges less than threshold from previous time step, 
						to remove the edges less than threshold including current time step
						 use the below condition and for loop in the end of this function*/
						if(cdw.getWeight()<THRESHOLD){
							toRmoveSet.add(og.getKey());
							
						}
					}
					
					for(String r : toRmoveSet)
					    edgeMap.remove(r);
				}

				if (edgeMap.containsKey(key)) {
					EdgeDataWeight cdw = edgeMap.get(key);
					cdw.setNum(cdw.getNum() + 1);
					cdw.setWeight(cdw.getWeight() + 1.0);
					
				} else {
					EdgeDataWeight cdw = new EdgeDataWeight();

					cdw.setNum(1);
					cdw.setWeight(1.0);
					edgeMap.put(key, cdw);
				}

				//System.out.println();
				
				/*for(Entry<String, CallDataWeight> og : edgeMap.entrySet()){
					System.out.println(og.getKey()+"|"+og.getValue().getWeight());
				}
				System.out.println("---------------------");*/
				
				currentDate=date;


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
	static SimpleDateFormat format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	static SimpleDateFormat format = new SimpleDateFormat("ddMMyyyy");

	private static String getDate(String date) {
		if (date != null)
			try {
				return format.format(format1.parse(date));
			} catch (ParseException e) {
				e.printStackTrace();
				return null;
			}
		else
			return null;
	}

	

}

class EdgeDataWeight {
	private String date;
	private int num;
	private double weight;

	public String getDate() {
		return date;
	}

	public void setDate(String date) {
		this.date = date;
	}

	public double getWeight() {
		return weight;
	}

	public void setWeight(double weight) {
		this.weight = weight;
	}

	public int getNum() {
		return num;
	}

	public void setNum(int num) {
		this.num = num;
	}
}
