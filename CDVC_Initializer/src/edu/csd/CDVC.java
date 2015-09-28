package edu.csd;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.List;

import org.json.simple.JSONObject;

import edu.csd.database.HBaseInstance;
import edu.csd.queue.RabbitMQInstance;

public class CDVC {

	public static final String storageConnectionString = "UseDevelopmentStorage=true";

	private final static String filesBlobName = "datasetcontainer";

	public static void main(String[] args) throws IOException {
		char c = '0';
		do {
			System.out
					.println("\n----------------------------------------------------------------------------");
			System.out.println("\nCDVC-Menu:");
			System.out
					.println("\n----------------------------------------------------------------------------");
			System.out.println("\n1. Load Dataset + Preprocessing.");
			System.out.println("\n2. Insert a new Image Descriptor Vector.");
			System.out.println("\n3. CDVC k-NN Similarity Queries From File.");
			System.out
					.println("\n----------------------------------------------------------------------------");
			System.out.println("\n0. Exit.");
			System.out
					.println("\n----------------------------------------------------------------------------\n\n");
			c = (char) System.in.read();
			if (c == '1') {
				executePreprocessing();
			} else if (c == '2') {
				executeInsertion();
			} else if (c == '3') {
				executeQuery();
			} else if (c == '0') {
				System.out.println("Bye");
			} else {
				System.out.println("Wrong Number. Please insert a new number!");
			}
		} while (c != '0');
	}

	private static void executeQuery() {

	}

	private static void executeInsertion() {
		// TODO Auto-generated method stub

	}

	private static void executePreprocessing() {
		// Load the file with the images descriptor vectors to the
		// blob storage to be retrieved by the CDVC_Scheduler

		// Request for the dataset file.
		String datasetFile = askForFile();
		String[] splitDatasetFilePath = datasetFile.split("\\");
		String cleanDatasetFileName = splitDatasetFilePath[splitDatasetFilePath.length];

		uploadDatasetFile(datasetFile, cleanDatasetFileName);
		sendMessage(cleanDatasetFileName, 1);
	}

	private static void sendMessage(String fileName, int functionality) {

		// Create the json object with the appropriate variables
		JSONObject obj = new JSONObject();
		obj.put("file", fileName);
		obj.put("functionality", new Integer(functionality));

		RabbitMQInstance rmq = new RabbitMQInstance();
		rmq.sendMessage(obj.toJSONString());

	}

	private static void uploadDatasetFile(String datasetFile,
			String cleanDatasetFileName) {
			HBaseInstance hbase = new HBaseInstance(cleanDatasetFileName);

			BufferedReader br = null;

			try {

				String sCurrentLine;

				br = new BufferedReader(new FileReader(datasetFile));

				int counter = 0;
				List<String> tuplesList = new ArrayList<>();
				while ((sCurrentLine = br.readLine()) != null) {
					counter++;
					tuplesList.add(sCurrentLine);
					if ((counter % 1000) == 0) {
						hbase.addToTable(tuplesList, counter);
						tuplesList.clear();
					}
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

	private static String askForFile() {
		System.out
				.println("Please insert the file with the images' descriptor vectors to be preprocessed");
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String datasetFile = null;
		try {
			datasetFile = br.readLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return datasetFile;
	}

}
