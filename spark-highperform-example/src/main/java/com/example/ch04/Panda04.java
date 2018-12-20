package com.example.ch04;

public class Panda04 {
	private String name;
	private float size;
	private String zip;

	public Panda04(String name, float size) {
		this.name = name;
		this.size = size;
	}

	public Panda04(String name, String zip) {
		this.name = name;
		this.zip = zip;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public float getSize() {
		return size;
	}

	public void setSize(float size) {
		this.size = size;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}
}
