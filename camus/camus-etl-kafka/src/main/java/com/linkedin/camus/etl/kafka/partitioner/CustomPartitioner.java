package com.linkedin.camus.etl.kafka.partitioner;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.Partitioner;

/*
 * @author Rachit Chauhan
 * */
public class CustomPartitioner extends Partitioner
{

	private static final org.apache.log4j.Logger log = Logger.getLogger(CustomPartitioner.class);
	@Override
	public String encodePartition(JobContext context, IEtlKey etlKey)
	{
		// TODO Auto-generated method stub
		log.info("Inside encodePartition");
		return etlKey.getServer();
	}

	@Override
	public String generatePartitionedPath(JobContext context, String topic, String encodedPartition)
	{
		// TODO Auto-generated method stub
		log.info("Inside generatePartitionedPath");
		String directoryName=topic+"/"+encodedPartition.split("_")[1];
		log.info("DRECTORY NAME:"+directoryName);
		return directoryName;
	}

	@Override
	public String generateFileName(JobContext context, String topic, String brokerId, int partitionId, int count, long offset, String encodedPartition)
	{
		// TODO Auto-generated method stub
		log.info("Inside generateFileName");
		//return encodedPartition.split("_")[2];
		String directoryName=topic+"/"+encodedPartition.split("_")[2];
		log.info("DRECTORY NAME:"+directoryName);
		return directoryName;
	}

	@Override
	public String getWorkingFileName(JobContext context, String topic, String brokerId, int partitionId, String encodedPartition)
	{
		// TODO Auto-generated method stub
		log.info("Inside getWorkingFileName");
		//return encodedPartition.split("_")[2];
		String directoryName=topic+"/"+encodedPartition.split("_")[2];
		log.info("DRECTORY NAME:"+directoryName);
		return directoryName;
	}

}
