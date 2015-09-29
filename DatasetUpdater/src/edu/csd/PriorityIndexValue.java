package edu.csd;

public class PriorityIndexValue{
	private int cardinalityValue;
	private int dimension;
	
	public PriorityIndexValue(int cardinalityValue, int dimension) {
		this.cardinalityValue = cardinalityValue;
		this.dimension = dimension;
	}
	
	public int getCardinalityValue() {
		return cardinalityValue;
	}
	
	public int getDimension() {
		return dimension;
	}
}
