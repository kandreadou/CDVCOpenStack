package edu.csd;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class ImageComparatorNode {

	private final static String imageComparatorQueue = "imagecomparatorqueue";
	private final static String distanceCollectorQueue = "distancecollectorqueue";
	private static String datasetName;
	private static int range;

	public static void main(String[] args) {
		RabbitMQInstance rmq = new RabbitMQInstance(imageComparatorQueue);
		while (true) {
			String message = rmq.getMessage();
			if (message != null) {

				// parse json string
				Object obj = JSONValue.parse(message);
				JSONObject jsonObject = (JSONObject) obj;
				datasetName = (String) jsonObject.get("dataset");
				int descriptorId = (int) jsonObject.get("descriptorId");
				int candidateId = (int) jsonObject.get("candidateId");
				range = (int) jsonObject.get("range");

				DescriptorVectorEntity queryEntity = retrieveEntity(descriptorId);
				DescriptorVectorEntity candidateEntity = retrieveEntity(candidateId);
				double result = compareImages(queryEntity, candidateEntity);

				sendMessageToDistanceCollector(result, candidateId);
			}
		}

	}

	private static void sendMessageToDistanceCollector(double result,
			int candidateId) {
		// Create the queue client
		RabbitMQInstance rmq = new RabbitMQInstance(distanceCollectorQueue);

		JSONObject obj = new JSONObject();
		obj.put("dataset", datasetName);
		obj.put("result", new Double(result));
		obj.put("candidateId", new Integer(candidateId));
		obj.put("range", new Integer(range));

		// Convert the list with the cardinality values to json string
		String jsonString = obj.toJSONString();

		// Send the Message
		rmq.sendMessage(jsonString);

	}

	private static double compareImages(DescriptorVectorEntity queryEntity,
			DescriptorVectorEntity candidateEntity) {
		String queryVector = queryEntity.getVector();
		String[] splitQueryVector = queryVector.split(",");

		String candidateVector = candidateEntity.getVector();
		String[] splitCandidateVector = candidateVector.split(",");

		double sum = 0.0;
		for (int i = 0; i < splitQueryVector.length; i++) {
			Double queryValue = Double.parseDouble(splitQueryVector[i]);
			Double candidateValue = Double.parseDouble(splitCandidateVector[i]);
			sum += Math.pow(queryValue - candidateValue, 2);
		}
		double result = Math.sqrt(sum);
		return result;
	}

	private static DescriptorVectorEntity retrieveEntity(int id) {
		HBaseInstance hbi = new HBaseInstance(datasetName);
		String descriptorVector = hbi.retrieveDescriptorVector(id);
		DescriptorVectorEntity entity = new DescriptorVectorEntity(id,
				descriptorVector);

		return entity;
	}

}
