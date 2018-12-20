package com.example.utils;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import com.example.ch03.PandaPlace;
import com.example.ch03.RawPanda;
import com.google.common.collect.Lists;

public class TestPandaProvider {
	public static List<PandaPlace> extract() {
		List<PandaPlace> places = Lists.newArrayList();
		String[] name = {"Beijing", "Seoul", "Tokyo"};
		String[] zip = {"94110","95420","96018"};
		String[] pt = {"giant","red"};

		for (int i = 0; i < 3; i++) {
			List<RawPanda> raws = Lists.newArrayList();
			for (int j = 0; j < 5; j++) {
				RawPanda raw = new RawPanda();
				raw.setId(new Long((i + 1) * 10 + j));
				raw.setHappy(j % 2 == 0);
				raw.setPt(pt[RandomUtils.nextInt(0, 2)]);
				raw.setZip(zip[RandomUtils.nextInt(0, 3)]);
				raw.setPandaSize(RandomUtils.nextInt(150, 200));
				raw.setAge(RandomUtils.nextInt(2, 10));
				raw.setAttributes(Lists.newArrayList((i+1)/(double)(i+2), (j+1)/(double)(j+2)));
				raws.add(raw);
			}
			places.add(new PandaPlace(name[RandomUtils.nextInt(0, 3)], raws));
		}
		return places;
	}
}
