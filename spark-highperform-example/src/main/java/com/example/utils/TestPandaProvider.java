package com.example.utils;

import java.util.List;

import org.apache.commons.lang3.RandomUtils;

import com.example.model.Goldilock;
import com.example.model.PandaPlace;
import com.example.model.RawPanda;
import com.google.common.collect.Lists;

public class TestPandaProvider {
	public static List<PandaPlace> extractPandaPlaces() {
		List<PandaPlace> places = Lists.newArrayList();
		String[] name = {"Happy", "Coffee", "Tea"};
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
	
	public static List<Goldilock> extractGoldilocks() {
		List<Goldilock> locks = Lists.newArrayList(
			new Goldilock("Mama Panda", 15.0d, 0.25d, 2467.0d, 0.0d),
			new Goldilock("Papa Panda", 2.0d, 1000.0d, 35.4d, 0.0d),
			new Goldilock("Baby Panda", 10.0d, 2.0d, 50.0d, 0.0d),
			new Goldilock("Baby Panda's toy Panda", 3.0d, 8.5d, 0.2d, 98.0d)
			);
		
		return locks;
	}
	
}
