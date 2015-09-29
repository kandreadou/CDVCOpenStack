package edu.csd;

import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class DistanceCollector {

	private final static String distanceCollectorQueue = "distancecollectorqueue";
	private static String datasetName;
	private static int range;
	private final static int topk = 100;
	private static PriorityQueue<ResultEntity> resultHeap;

	public static void main(String[] args) {
		RabbitMQInstance rmq = new RabbitMQInstance(distanceCollectorQueue);

		resultHeap = new PriorityQueue<>(2 * range,
				new Comparator<ResultEntity>() {

					@Override
					public int compare(ResultEntity o1, ResultEntity o2) {
						if (o1.getResult() > o2.getResult())
							return -1;
						if (o1.getResult() < o2.getResult())
							return 1;
						return 0;
					}
				});
		while (true) {
			String message = rmq.getMessage();
			if (message != null) {

				// parse json string
				Object obj = JSONValue.parse(message);
				JSONObject jsonObject = (JSONObject) obj;
				datasetName = (String) jsonObject.get("dataset");
				double result = (double) jsonObject.get("result");
				int candidateId = (int) jsonObject.get("candidateId");
				range = (int) jsonObject.get("range");

				ResultEntity entity = new ResultEntity(candidateId, result);
				resultHeap.offer(entity);

				// If all the candidate descriptor vectors have been compared
				if (resultHeap.size() == (2 * range)) {
					storeTheResultSet();
				}
			}

		}

	}

	private static void storeTheResultSet() {
		List<ResultEntity> entityList = new ArrayList<>();
		for (int i = 0; i < topk; i++) {
			entityList.add(resultHeap.poll());
		}

		RSetEntity rSet = new RSetEntity(datasetName,
				new JSONValue().toJSONString(entityList));
		HBaseInstance hbi = new HBaseInstance(datasetName + "result");
		try {
			hbi.addResult(rSet);
		} catch (RetriesExhaustedWithDetailsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
