package com.example.ch03;

import java.util.List;

public class PandaPlace {
	private String name;
	private List<RawPanda> pandas;
	
	public PandaPlace() {}
	
	public PandaPlace(String name, List<RawPanda> pandas) {
		this.name = name;
		this.pandas = pandas;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<RawPanda> getPandas() {
		return pandas;
	}

	public void setPandas(List<RawPanda> pandas) {
		this.pandas = pandas;
	}

}