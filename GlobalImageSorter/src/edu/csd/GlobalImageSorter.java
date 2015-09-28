package edu.csd;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class GlobalImageSorter {

	private final static String globalImageSorterQueue = "globalimagesorterqueue";
	private final static String imageSorterToGlobalImageSorterQueue = "imagesortertoglobalqueue";
	private static List<List<Integer>> Llists;
	private static List<Integer> globalL;
	private static List<PriorityIndexValue> priorityIndex;
	private static String datasetName;
	private static int numOfImages;

	public static void main(String[] args) {
		// globalL contains the ids of the descriptor vectors into their
		// reordered position
		globalL = new ArrayList<>();

		priorityIndex = new ArrayList<>();

		// initialize the globalImageSorterQueue which retrieves the message
		// from
		// the priorityIndexer
		RabbitMQInstance rmqGlobal = new RabbitMQInstance(
				globalImageSorterQueue);
		while (true) {
			String message = rmqGlobal.getMessage();
			if (message != null) {

				// parse json string
				Object obj = JSONValue.parse(message);
				JSONObject jsonObject = (JSONObject) obj;
				datasetName = (String) jsonObject.get("dataset");
				int numOfL = (Integer) jsonObject.get("numOfL");
				numOfImages = (Integer) jsonObject.get("numOfImages");

				Llists = new ArrayList<>();
				// initialize the imageSorterToGlobalImageSorterQueue which
				// retrieves the
				// L^{(m)}
				RabbitMQInstance rmqImageSorter = new RabbitMQInstance(
						imageSorterToGlobalImageSorterQueue);

				while (Llists.size() != numOfL) {
					message = rmqImageSorter.getMessage();
					if (message != null) {

						// parse json string
						obj = JSONValue.parse(message);
						JSONArray array = (JSONArray) obj;
						List<Integer> tempLList = new ArrayList<>();

						for (int i = 0; i < array.size(); i++) {
							tempLList.add((int) array.get(i));
						}
						Llists.add(tempLList);
					}
				}

				retrieveThePriorityIndexer();

				sortImages();

				storeTheUpdatedPositionList();
			}

		}
	}

	private static void storeTheUpdatedPositionList() {
		HBaseInstance hbi = new HBaseInstance(datasetName + "positions");
		try {
			hbi.addPositionList(JSONValue.toJSONString(globalL));
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

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

	private static void sortImages() {
		while (globalL.size() < numOfImages) {
			List<TempDescriptorVectorEntity> tempImagesList = new ArrayList<>();
			for (int i = 0; i < Llists.size(); i++) {
				if (!Llists.get(i).isEmpty()) {
					DescriptorVectorEntity entity = retrieveDescriptorVector(Llists
							.get(i).get(0));
					TempDescriptorVectorEntity tempEntity = new TempDescriptorVectorEntity(
							entity.getId(), entity.getVector(), i);
					tempImagesList.add(tempEntity);

					// Create a new GlobalDescriptorVectorEntity where the
					// partition key is the dimension with the highest priority
					String vector = entity.getVector();
					String[] splitVector = vector.split(",");
					String partitionKey = splitVector[priorityIndex.get(0)
							.getDimension()];
					GlobalDescriptorVectorEntity globalEntity = new GlobalDescriptorVectorEntity(
							partitionKey, entity.getId(), entity.getVector());
					storeTheGlobalEntity(globalEntity);
				}
			}

			// Sort the descriptor vectors in descending order based on
			// the priority index;
			Collections.sort(tempImagesList,
					new Comparator<TempDescriptorVectorEntity>() {

						@Override
						public int compare(TempDescriptorVectorEntity o1,
								TempDescriptorVectorEntity o2) {
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

			// Get the descriptor vector with the highest value;
			globalL.add(tempImagesList.get(0).getId());

			// remove the id from the corresponding L^{(m)} list
			Llists.get(tempImagesList.get(0).getImageSorterNode()).remove(0);

		}
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

	private static DescriptorVectorEntity retrieveDescriptorVector(
			int descriptorVectorId) {
		HBaseInstance hbi = new HBaseInstance(datasetName);
		String descriptorVector = hbi
				.retrieveDescriptorVector(descriptorVectorId);
		DescriptorVectorEntity entity = new DescriptorVectorEntity(
				descriptorVectorId, descriptorVector);

		return entity;
	}

}
