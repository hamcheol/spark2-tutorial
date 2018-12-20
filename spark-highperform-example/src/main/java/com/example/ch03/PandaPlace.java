package com.example.ch03;

import java.io.Serializable;
import java.util.List;

public class PandaPlace implements Serializable {
	private static final long serialVersionUID = -5020718366138535119L;
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