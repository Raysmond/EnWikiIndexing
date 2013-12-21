package com.raysmond.wiki.job;

public class Main {
	public static void main(String[] args) throws Exception {
		HWikiIndexJob job = new HWikiIndexJob();

		if (args.length >= 1)
			job.setInputPath(args[0]);
		if (args.length >= 2)
			job.setOutputPath(args[1]);

		job.call();
		//job.getRunningJob().waitForCompletion();
	}
}
