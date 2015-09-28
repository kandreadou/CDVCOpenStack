package edu.csd;


public class DescriptorVectorEntity{
	private String fileName;
	private int id;
	private String vector;
	
	public DescriptorVectorEntity(String filename, int id, String vector) {
		this.fileName = filename;
		this.id = id;
		this.vector = vector;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
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
