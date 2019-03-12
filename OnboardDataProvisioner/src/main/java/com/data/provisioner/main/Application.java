package com.data.provisioner.main;

import com.data.provisioner.train.impl.Train;

public class Application {

	public static void main(final String[] arguments) throws InterruptedException {
		final Train train = new Train();
		train.start();
//		Thread.sleep(15000);
//		train.stop();
	}

}
