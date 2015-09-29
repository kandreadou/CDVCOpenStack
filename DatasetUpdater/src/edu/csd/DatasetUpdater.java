package edu.csd;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class DatasetUpdater {

	private final static String insertionQueue = "insertionqueue";
	private final static String candidateImageIDRetrievalQueue = "candidateidretrievalqueue";
	private static List<Integer> positionsList = new ArrayList<>();
	private static List<Integer> updatedPositionsList = new ArrayList<>();
	private static String datasetName;
	private static List<PriorityIndexValue> priorityIndex;
	private static String query;
	private static Double W;
	private static int N;


	public static void main(String[] args) {

		priorityIndex = new ArrayList<>();
		RabbitMQInstance rmq = new RabbitMQInstance(insertionQueue);
		while (true) {

			String message = rmq.getMessage();
			if (message != null) {

				// parse json string
				Object obj = JSONValue.parse(message);
				JSONObject jsonObject = (JSONObject) obj;
				datasetName = (String) jsonObject.get("dataset");
				String jsonIdList = (String) jsonObject.get("idlist");
				query = (String) jsonObject.get("query");
				N = (Integer) jsonObject.get("numOfImages");
				W = (Double) jsonObject.get("W");

				retrieveThePriorityIndexer();

				retrieveTheDescriptorVectorsPositions();

				insertImageDescriptorVector();

			}

		}
	}

	private static void sendMessageToQueryProcessingStep(int id, int position) {
		// Create the queue client
		RabbitMQInstance rmq = new RabbitMQInstance(
				candidateImageIDRetrievalQueue);

		JSONObject obj = new JSONObject();
		obj.put("dataset", datasetName);
		obj.put("numOfImages", new Integer(N));
		obj.put("descriptorId", new Integer(id));
		obj.put("W", new Double(W));
		obj.put("position", new Integer(position));

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

	private static void insertImageDescriptorVector() {
		DescriptorVectorEntity entity = new DescriptorVectorEntity(0, query);
		List<GlobalDescriptorVectorEntity> candidateEntities = retrieveCandidateEntities(entity);

		int imageId = -1;
		int max = -1;
		// Identify which image has the highest position
		for (int j = 0; j < candidateEntities.size(); j++) {
			for (int k = 0; k < positionsList.size(); k++) {
				if (candidateEntities.get(j).getId() == positionsList.get(k)) {
					if (k > max) {
						max = k;
						imageId = j;
					}
				}
			}
		}

		// Update the position list according to the comparison result
		int result = compareImages(candidateEntities.get(imageId), entity);
		int position;
		if (result > 0) {
			for (int j = 0; j < (max - 1); j++) {
				updatedPositionsList.add(positionsList.get(j));
			}
			updatedPositionsList.add(entity.getId());
			position = updatedPositionsList.size() - 1;
			updatedPositionsList.add(positionsList.get(max));
			for (int j = (max + 1); j < positionsList.size(); j++) {
				updatedPositionsList.add(positionsList.get(j));
			}
		} else {
			for (int j = 0; j < max; j++) {
				updatedPositionsList.add(positionsList.get(j));
			}
			updatedPositionsList.add(entity.getId());
			position = updatedPositionsList.size() - 1;
			for (int j = (max + 1); j < positionsList.size(); j++) {
				updatedPositionsList.add(positionsList.get(j));
			}
		}

		storeTheUpdatedPositionList();

		indexTheInsertedImage(entity);

		if (query.equals("y")) {
			sendMessageToQueryProcessingStep(entity.getId(), position);
		}

	}

	private static void indexTheInsertedImage(DescriptorVectorEntity entity) {
		String insertedVector = entity.getVector();
		String[] insertedSplitVector = insertedVector.split(",");
		GlobalDescriptorVectorEntity globalEntity = new GlobalDescriptorVectorEntity(
				insertedSplitVector[priorityIndex.get(0).getDimension()],
				entity.getId(), entity.getVector());
		storeTheGlobalEntity(globalEntity);
	}

	private static void storeTheGlobalEntity(GlobalDescriptorVectorEntity entity) {
		HBaseInstance hbi = new HBaseInstance(datasetName + "ordered");
		try {
			hbi.addToTable(entity.getVector(),
					entity.getDescriptorVectorPartitionKey(), entity.getId());
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static void storeTheUpdatedPositionList() {
		HBaseInstance hbi = new HBaseInstance(datasetName + "positions");
		try {
			hbi.addPositionList(new JSONValue()
					.toJSONString(updatedPositionsList));
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static int compareImages(
			GlobalDescriptorVectorEntity globalDescriptorVectorEntity,
			DescriptorVectorEntity entity) {
		String candidateVector = globalDescriptorVectorEntity.getVector();
		String[] candidateSplitVector = candidateVector.split(",");

		String insertedVector = entity.getVector();
		String[] insertedSplitVector = insertedVector.split(",");

		for (int i = 0; i < insertedSplitVector.length; i++) {
			double insertedValue = Double
					.parseDouble(insertedSplitVector[priorityIndex.get(i)
							.getDimension()]);
			double candidateValue = Double
					.parseDouble(candidateSplitVector[priorityIndex.get(i)
							.getDimension()]);
			if (insertedValue > candidateValue) {
				return 1;
			} else if (insertedValue < candidateValue) {
				return -1;
			}
		}
		return 0;
	}

	private static List<GlobalDescriptorVectorEntity> retrieveCandidateEntities(
			DescriptorVectorEntity entity) {
		String vector = entity.getVector();
		String[] splitVector = vector.split(",");
		String primaryKey = splitVector[priorityIndex.get(0).getDimension()];
		HBaseInstance hbi = new HBaseInstance(datasetName + "ordered");
		return hbi.retrieveCandidateDescriptorVectors(primaryKey);

	}

	private static DescriptorVectorEntity retrieveDescriptorVector(
			int descriptorVectorId) {
		HBaseInstance hbi = new HBaseInstance(datasetName);
		String descriptorVector = hbi
				.retrieveDescriptorVector(descriptorVectorId);
		DescriptorVectorEntity entity = new DescriptorVectorEntity(
				descriptorVectorId, descriptorVector);

		return entity;
	}

	private static void retrieveThePriorityIndexer() {
		HBaseInstance hbi = new HBaseInstance(datasetName + "priority");
		List<String> priorityIndexList = hbi.retrievePriorityIndex();
		String priorityIndexString = priorityIndexList.get(0);
		String[] priorityIndexSplit = priorityIndexString.split(",");
		for (int i = 0; i < priorityIndexSplit.length; i++) {
			priorityIndex.add(new PriorityIndexValue(0, Integer
					.parseInt(priorityIndexSplit[i])));
		}

	}
}
