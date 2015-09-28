package edu.csd;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class PriorityIndexer {

	private final static String priorityIndexQueue = "priorityindexqueue";
	private final static String dvcPriorityIndexQueue = "dvcpriorityindexqueue";
	private final static String imageSorterNodesQueue = "imagesorterqueue";
	private final static String globalImageSorterQueue = "globalimagesorterqueue";
	private static List<List<Integer>> cardinalityValuesList;
	private static List<PriorityIndexValue> priorityIndex;
	private static String datasetName;
	private static int numOfImages, numberOfImageSorterNodes = 2;

	public static void main(String[] args) {
		// initialize the priorityindexqueue which retrieves the message from
		// the scheduler
		RabbitMQInstance schedulerRMQ = new RabbitMQInstance(priorityIndexQueue);

		while (true) {
				
				String message = schedulerRMQ.getMessage();
				if (message != null) {
					// parse json string
					Object obj = JSONValue.parse(message);
					JSONObject jsonObject = (JSONObject) obj;
					datasetName = (String) jsonObject.get("dataset");
					int numOfC = (Integer) jsonObject.get("numOfC");
					numOfImages = (Integer) jsonObject.get("numOfImages");

					cardinalityValuesList = new ArrayList<>();
					// initialize the dvcpriorityindexqueue which retrieves the
					// L^{(m)}
					RabbitMQInstance dvcRMQ = new RabbitMQInstance(dvcPriorityIndexQueue);

					while (cardinalityValuesList.size() != numOfC) {
						message = dvcRMQ.getMessage();
						if (message != null) {
							// parse json string
							obj = JSONValue.parse(message);
							JSONArray array = (JSONArray) obj;
							List<Integer> tempCardinalityList = new ArrayList<>();

							for (int i = 0; i < array.size(); i++) {
								tempCardinalityList.add((int) array.get(i));
							}
							cardinalityValuesList.add(tempCardinalityList);
						}
					}
					generatePriorityIndex();

					// Store the priorityIndex to the azure table
					storePriorityIndex();

					// Split the images' descriptor vectors to M image sorter
					// nodes to start the comparison
					sortImagesDescriptorVectors();
					
					//Send message to the Global Image Sorter to retrieve the numberOfImageSorterNodes L^{(m)}
					initializeGlobalImageSorter();
				}
			
		}
	}

	private static void initializeGlobalImageSorter() {
		
			// Create the queue client
			RabbitMQInstance rmq = new RabbitMQInstance(globalImageSorterQueue);


			// Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("numOfL", new Integer(numberOfImageSorterNodes));
			obj.put("numOfImages", new Integer(numOfImages));

			// Send the Message
			rmq.sendMessage(obj.toString());
			


	}

	private static void sortImagesDescriptorVectors() {
		int step = (int) numOfImages / numberOfImageSorterNodes;
		for (int i = 0; i < numberOfImageSorterNodes; i++) {
			int startImageId = (i * step) + 1;
			int stopImageId = (i * step) + step;
			// Initialize the DVCExtractor Nodes
			sendMessageToImageSorterNodes(startImageId, stopImageId);
		}
	}

	private static void sendMessageToImageSorterNodes(int startImageId,
			int stopImageId) {
			// Create the queue client
			RabbitMQInstance rmq = new RabbitMQInstance(imageSorterNodesQueue);
			

			// Create the json object with the appropriate variables
			JSONObject obj = new JSONObject();
			obj.put("dataset", datasetName);
			obj.put("startImageId", new Integer(startImageId));
			obj.put("stopImageId", new Integer(stopImageId));
			
			rmq.sendMessage(obj.toString());
			rmq.sendMessage(obj.toString());
	}

	private static void storePriorityIndex() {
		HBaseInstance priorityHBase = new HBaseInstance(datasetName +"priority");
		try {
			priorityHBase.addToTable(priorityIndex, 1);
		} catch (RetriesExhaustedWithDetailsException | InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static void generatePriorityIndex() {
		int dimensions = cardinalityValuesList.get(0).size();
		priorityIndex = new ArrayList<>();
		// aggregate the M C^{(m)} cardinality values vectors to create the
		// priority indexer
		for (int i = 0; i < cardinalityValuesList.size(); i++) {
			for (int j = 0; j < dimensions; j++) {
				int value = cardinalityValuesList.get(i).get(j);
				if (value != 0) {
					PriorityIndexValue entity = new PriorityIndexValue(value, j);
					priorityIndex.add(entity);
				}
			}
		}

		// Sort the cardinality Values of each dimension in descending order
		Collections.sort(priorityIndex);
	}
}
