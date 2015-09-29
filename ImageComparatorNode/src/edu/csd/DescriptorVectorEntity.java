package edu.csd;

public class DescriptorVectorEntity {
	private int id;
	private String vector;

	public DescriptorVectorEntity(int id, String vector) {
		this.id = id;
		this.vector = vector;
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
