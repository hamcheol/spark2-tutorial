package com.example;

import java.io.*;
import java.math.*;
import java.security.*;
import java.text.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

import org.apache.commons.lang3.RandomUtils;

public class Solution {

	// Complete the diagonalDifference function below.
	/*
	a[0][0]
	a[1][1]
	a[2][2]
	b[0][2]
	b[1][1]
	b[2][0]
	*/

	static int diagonalDifference(int[][] arr) {
		int n = arr.length - 1;
		int diagonalOne = 0;
		int diagonalTwo = 0;
		for (int i = 0; i <= n; i++) {
			diagonalOne += arr[i][i];
			diagonalTwo += arr[i][n - i];
			System.out.println(arr[i][i] + "|" + arr[i][n - i]);
			System.out.println(diagonalOne + ":" + diagonalTwo);
		}
		return diagonalOne - diagonalTwo;
	}

	private static final Scanner scanner = new Scanner(System.in);

	public static void main(String[] args) throws IOException {
		int[][] arr = new int[3][3];

		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				arr[i][j] = RandomUtils.nextInt(0, 10);
			}
		}
		
		for (int i = 0; i < 3; i++) {
			for (int j = 0; j < 3; j++) {
				System.out.print(arr[i][j] + " ");
			}
			System.out.println("\n");
		}

		int result = diagonalDifference(arr);
		
		System.out.println(result);


		scanner.close();
	}
}
