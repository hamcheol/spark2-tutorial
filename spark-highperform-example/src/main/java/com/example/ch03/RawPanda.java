package com.example.ch03;

import java.util.List;

public class RawPanda {
	private Long id;
	private String zip;
	private String pt;
	private Boolean happy;
	private List<Double> attributes;

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public String getZip() {
		return zip;
	}

	public void setZip(String zip) {
		this.zip = zip;
	}

	public Boolean getHappy() {
		return happy;
	}

	public void setHappy(Boolean happy) {
		this.happy = happy;
	}

	public List<Double> getAttributes() {
		return attributes;
	}

	public void setAttributes(List<Double> attributes) {
		this.attributes = attributes;
	}

	public String getPt() {
		return pt;
	}

	public void setPt(String pt) {
		this.pt = pt;
	}

}
