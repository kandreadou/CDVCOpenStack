package edu.csd;


public class RSetEntity{
	private String datasetName;
	private String R;
	
	public RSetEntity(String datasetName, String R) {
		this.datasetName = datasetName;
		this.R = R;
	}

	public String getDatasetName() {
		return datasetName;
	}

	public void setDatasetName(String datasetName) {
		this.datasetName = datasetName;
	}

	public String getR() {
		return R;
	}

	public void setR(String r) {
		R = r;
	}
}
