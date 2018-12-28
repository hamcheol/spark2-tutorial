package com.example.model;

public class Goldilock {
	private String name;
	private Double happiness;
	private Double niceness;
	private Double softness;
	private Double sweetness;

	public Goldilock() {}

	public Goldilock(String name, Double happiness, Double niceness, Double softness, Double sweetness) {
		this.name = name;
		this.happiness = happiness;
		this.niceness = niceness;
		this.softness = softness;
		this.sweetness = sweetness;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Double getHappiness() {
		return happiness;
	}

	public void setHappiness(Double happiness) {
		this.happiness = happiness;
	}

	public Double getNiceness() {
		return niceness;
	}

	public void setNiceness(Double niceness) {
		this.niceness = niceness;
	}

	public Double getSoftness() {
		return softness;
	}

	public void setSoftness(Double softness) {
		this.softness = softness;
	}

	public Double getSweetness() {
		return sweetness;
	}

	public void setSweetness(Double sweetness) {
		this.sweetness = sweetness;
	}

}
