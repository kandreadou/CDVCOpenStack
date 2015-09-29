package edu.csd;

public class GlobalDescriptorVectorEntity {

	private int id;
	private String partitionKey;
	private String vector;

	public GlobalDescriptorVectorEntity(String descriptorVectorPartitionKey,
			int id, String vector) {
		this.partitionKey = descriptorVectorPartitionKey;
		this.id = id;
		this.vector = vector;
	}

	public String getDescriptorVectorPartitionKey() {
		return partitionKey;
	}

	public void setDescriptorVectorPartitionKey(
			String descriptorVectorPartitionKey) {
		this.partitionKey = descriptorVectorPartitionKey;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getVector() {
		return vector;
	}

	public void setVector(String vector) {
		this.vector = vector;
	}
}
