package com.example.spark.rdd.book.nytimes;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Data;

@Data
public class Result {
	@JsonProperty("list_name")
	private String listName;
	@JsonProperty("bestsellers_date")
	private String bestsellersDate;
	@JsonProperty("published_date")
	private String publishedDate;
	private List<Book> books;
}
