package edu.csd;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;


public class DVCExtractor {

	private final static String dvcExtractorQueue = "dvcextractorqueue";
	private final static String priorityIndexQueue = "dvcpriorityindexqueue";
	private static HashMap<Integer, HashSet<Double>> cardinalityMap;
	private static List<Integer> cardinalityValues;
	public static void main(String[] args) {
		cardinalityMap = new HashMap<>();
		RabbitMQInstance rmq = new RabbitMQInstance(dvcExtractorQueue);
		while (true) {
				String message = rmq.getMessage();
				if (message != null) {

					// parse json string
					Object obj = JSONValue.parse(message);
					JSONObject jsonObject = (JSONObject) obj;
					String datasetName = (String) jsonObject.get("dataset");
					int N = (Integer) jsonObject.get("size");
					int startDimension = (Integer) jsonObject
							.get("startDimension");
					int stopDimension = (Integer) jsonObject
							.get("stoDimension");

					calculateCardinalityValues(datasetName, N, startDimension,
							stopDimension);
					
					sendResultToPriorityIndex();
				}
		}
	}

	private static void sendResultToPriorityIndex() {

			// Create the queue client
			RabbitMQInstance rmq = new RabbitMQInstance(priorityIndexQueue);
			
			
			//Convert the list with the cardinality values to json string
			String jsonString = JSONValue.toJSONString(cardinalityValues);
			
			rmq.sendMessage(jsonString);
			

	}

	private static void calculateCardinalityValues(String datasetName, int N,
			int startDimension, int stopDimension) {
		
		
		// Retrieve storage account from connection-string

			
			//contains the dimension of the descriptor vector
			int D = 0;
			HBaseInstance hbi = new HBaseInstance(datasetName);
			List<String> descriptorVectors = hbi.retrieveDescriptorVector(N);
			for (int i = 0; i <= descriptorVectors.size(); i++) {

				String descriptorVector = descriptorVectors.get(i);
				//Split the descriptor vector to process the specific dimensions
				String[] descriptors = descriptorVector.split(",");
				D = descriptors.length;
				for(int j = startDimension; j < stopDimension; j++) {
					double value = Double.parseDouble(descriptors[j]);
					if(!cardinalityMap.containsKey(j)) {
						HashSet<Double> dimensionCardinalityValue = new HashSet<>();
						dimensionCardinalityValue.add(value);
						cardinalityMap.put(j, dimensionCardinalityValue);
					} else {
						if(!cardinalityMap.get(j).contains(value)) {
							cardinalityMap.get(j).add(value);
						}
					}
				}
			}
			
			cardinalityValues = new ArrayList<>();
			int cardinalityValueScore = 0;
			for(int i = 0; i < D; i++) {
				if(cardinalityMap.containsKey(i)) {
					cardinalityValueScore = cardinalityMap.get(i).size();
				} else {
					cardinalityValueScore = 0;
				}
				cardinalityValues.add(cardinalityValueScore);
			}


	}

}
