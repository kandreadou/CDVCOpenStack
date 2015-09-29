package edu.csd;

import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class CandidateIdsRetrieval {

	private final static String candidateImageIDRetrievalQueue = "candidateidretrievalqueue";
	private final static String imageComparatorQueue = "imagecomparatorqueue";
	private static List<Integer> positionsList = new ArrayList<>();
	private static String datasetName;
	private static Double W;
	private static int N, descriptorId, position, range;

	public static void main(String[] args) {
		RabbitMQInstance rmq = new RabbitMQInstance(
				candidateImageIDRetrievalQueue);
		while (true) {
			String message = rmq.getMessage();
			if (message != null) {

				// parse json string
				Object obj = JSONValue.parse(message);
				JSONObject jsonObject = (JSONObject) obj;
				datasetName = (String) jsonObject.get("dataset");
				N = (Integer) jsonObject.get("numOfImages");
				W = (Double) jsonObject.get("W");
				descriptorId = (int) jsonObject.get("descriptorId");
				position = (int) jsonObject.get("position");

				// retrieve the positions of the descriptor vectors to
				// identify the candidate descriptor vectors
				retrieveTheDescriptorVectorsPositions();

				// Calculate the W range
				range = (int) ((int) N * W);
				for (int i = (position - range); i < position; i++) {
					sendMessageToComparator(positionsList.get(i));
				}

				for (int i = (position + 1); i < (position + range + 1); i++) {
					sendMessageToComparator(positionsList.get(i));
				}
			}

		}

	}

	private static void sendMessageToComparator(int candidateId) {

		// Create the queue client
		RabbitMQInstance rmq = new RabbitMQInstance(imageComparatorQueue);

		JSONObject obj = new JSONObject();
		obj.put("dataset", datasetName);
		obj.put("descriptorId", new Integer(descriptorId));
		obj.put("candidateId", new Integer(candidateId));
		obj.put("range", new Integer(range));

		// Convert the list with the cardinality values to json string
		String jsonString = obj.toJSONString();
		rmq.sendMessage(jsonString);

	}

	private static void retrieveTheDescriptorVectorsPositions() {
		HBaseInstance hbi = new HBaseInstance(datasetName + "positions");
		String positionListString = hbi.retrievePositionList();
		Object obj = JSONValue.parse(positionListString);
		JSONArray array = (JSONArray) obj;
		for (int i = 0; i < array.size(); i++) {
			positionsList.add((int) array.get(i));
		}
	}

}
