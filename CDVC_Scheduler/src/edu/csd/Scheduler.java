package edu.csd;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.queue.RabbitMQInstance;

public class Scheduler {

	private final static int numberOfDVCExtractorNodes = 4;
	private final static double W = 0.0025;

	private final static String schedulerQueue = "schedulerqueue";
	private final static String dvcExtractorQueue = "dvcextractorqueue";
	private final static String insertionQueue = "insertionqueue";
	private final static String priorityIndexerQueue = "priorityindexqueue";

	private static int D, N;
	private static String datasetName;

	public static void main(String[] args) {
		RabbitMQInstance rmq = new RabbitMQInstance(schedulerQueue);
		// Scheduler always is waiting for new requests-messages
		while (true) {
			String message = rmq.getMessage();
			if (message != null) {
				String json = message;

				// parse json string
				Object obj = JSONValue.parse(json);
				JSONObject jsonObject = (JSONObject) obj;
				String fileName = (String) jsonObject.get("file");
				int functionality = (Integer) jsonObject.get("functionality");

				if (functionality == 1) {
					// Initialize the preprocessing step.
					intializePreprocessing(fileName);
				} else if (functionality == 2) {
					// Initialize the insertion step.
					initializeInsertion(fileName, "n");
				} else if (functionality == 3) {
					initializeInsertion(fileName, "y");
				}
			}

		}
	}

	private static void initializeInsertion(String fileName, String query) {

		// send message to the Dataset Updater component
		sendMessageToInsertionComponent(fileName, query);

	}

	private static void sendMessageToInsertionComponent(String fileName,
			String query) {

		// Create the queue client
		RabbitMQInstance rmq = new RabbitMQInstance(insertionQueue);

		// Create the json object with the appropriate variables
		JSONObject obj = new JSONObject();
		obj.put("dataset", datasetName);
		obj.put("query", query);
		obj.put("numOfImages", new Integer(N));
		obj.put("W", new Double(W));

		// Send the Message
		rmq.sendMessage(obj.toString());
	}

	private static void intializePreprocessing(String fileName) {

		// send message to the priority indexer to define the number of
		// generated c^m cardinality value vectors
		initializePriorityIndexer();

		// send messages and start the preprocessing step
		startPreprocessing();

	}

	private static void initializePriorityIndexer() {
		// Create the queue client
		RabbitMQInstance rmq = new RabbitMQInstance(priorityIndexerQueue);

		// Create the json object with the appropriate variables
		JSONObject obj = new JSONObject();
		obj.put("dataset", datasetName);
		obj.put("numOfImages", new Integer(N));
		obj.put("numOfC", new Integer(numberOfDVCExtractorNodes));

		// Send the Message
		rmq.sendMessage(obj.toString());

	}

	private static void startPreprocessing() {
		int step = (int) D / numberOfDVCExtractorNodes;
		for (int i = 0; i < numberOfDVCExtractorNodes; i++) {
			int startDimension = (i * step) + 1;
			int stopDimension = (i * step) + step;
			// Initialize the DVCExtractor Nodes
			sendMessageToDVCExtractorNodes(startDimension, stopDimension);
		}
	}

	private static void sendMessageToDVCExtractorNodes(int startDimension,
			int stopDimension) {
		// Create the queue client
		RabbitMQInstance rmq = new RabbitMQInstance(dvcExtractorQueue);

		// Create the json object with the appropriate variables
		JSONObject obj = new JSONObject();
		obj.put("dataset", datasetName);
		obj.put("size", new Integer(N));
		obj.put("startDimension", new Integer(startDimension));
		obj.put("stopDimension", new Integer(stopDimension));

		// Send the Message
		rmq.sendMessage(obj.toString());

	}

}
