package edu.csd;

import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class ImageSorter {

	private final static String imageSorterNodesQueue = "imagesorterqueue";
	private final static String imageSorterToGlobalImageSorterQueue = "imagesortertoglobalqueue";
	private final static String priorityIndexTable = "priorityindex";
	private static List<DescriptorVectorEntity> descriptorList;
	private static List<PriorityIndexValue> priorityIndex;
	private static String datasetName;
	private static List<Integer> L;

	public static void main(String[] args) {
		L = new ArrayList<>();
		descriptorList = new ArrayList<>();
		priorityIndex = new ArrayList<>();
		// initialize the imagesorterqueue which retrieves the message from
		// the priorityindexer
		RabbitMQInstance rmq = new RabbitMQInstance(imageSorterNodesQueue);
		while (true) {
				String message = rmq.getMessage();
				if (message != null) {
					// parse json string
					Object obj = JSONValue.parse(message);
					JSONObject jsonObject = (JSONObject) obj;
					datasetName = (String) jsonObject.get("dataset");
					int startImageId = (Integer) jsonObject.get("startImageId");
					int stopImageId = (Integer) jsonObject.get("stopImageId");

					retrieveTheDescriptorVectors(startImageId, stopImageId);

					retrieveThePriorityIndexer();

					// Sort the descriptor vectors in descending order based on
					// the priority index;
					Collections.sort(descriptorList,
							new Comparator<DescriptorVectorEntity>() {

								@Override
								public int compare(DescriptorVectorEntity o1,
										DescriptorVectorEntity o2) {
									String vector1 = o1.getVector();
									String vector2 = o2.getVector();
									String[] splitVector1 = vector1.split(",");
									String[] splitVector2 = vector2.split(",");
									for (int i = 0; i < priorityIndex.size(); i++) {
										double value1 = Double
												.parseDouble(splitVector1[priorityIndex
														.get(i).getDimension()]);
										double value2 = Double
												.parseDouble(splitVector2[priorityIndex
														.get(i).getDimension()]);
										if (value1 > value2) {
											return 1;
										} else if (value1 < value2) {
											return -1;
										} else {
											return 0;
										}
									}
									return 0;
								}

							});

					// Generate the L^{(m)}
					for (int i = 0; i < descriptorList.size(); i++) {
						L.add(descriptorList.get(i).getId());
					}

					// Send the L^{(m)} to the global image sorter
					sendMessageToGlobalImageSorter();
				}

		}

	}

	private static void sendMessageToGlobalImageSorter() {


			// Create the queue client
			RabbitMQInstance rmq = new RabbitMQInstance(imageSorterToGlobalImageSorterQueue);
			
			// Convert the list with the cardinality values to json string
			String jsonString = JSONValue.toJSONString(L);

			// Send the Message
			rmq.sendMessage(jsonString);

	
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

	private static void retrieveTheDescriptorVectors(int startImageId,
			int stopImageId) {
		HBaseInstance hbi = new HBaseInstance(datasetName);
		List<String> descriptorVectors = hbi.retrieveDescriptorVector(
				startImageId, stopImageId);

		int counter = 0;
		for (int i = startImageId; i <= stopImageId; i++) {
			DescriptorVectorEntity entity = new DescriptorVectorEntity(
					datasetName, i, descriptorVectors.get(counter++));
			descriptorList.add(entity);
		}

	}
}
