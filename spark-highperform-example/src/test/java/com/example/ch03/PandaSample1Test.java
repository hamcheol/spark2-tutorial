package com.example.ch03;

import org.junit.Test;

public class PandaSample1Test {
	@Test
	public void test() {
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 5; j++) {
				System.out.println(((i+1) / (double)(i+2)));
				System.out.println(((j+2) / (double)(j+3)));
			}
		}
	}
	
	@Test
	public void test2() {
		System.out.println((double)(0 + 1 / 0 + 2));
		System.out.println((double)(0 + 2 / 0 + 3));
	}

}
