package com.hazelcast.demo.hadoop;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.parquet.avro.AvroParquetInputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.hadoop.HadoopSinks;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.file.FileFormat;
import com.hazelcast.jet.pipeline.file.FileSources;

/**
 * A pipeline which streams events from an IMap. The
 * values for new entries are extracted and then written
 * to a local IList in the Jet cluster.
 */
public class MapJournalToHadoop {

    private static final String MAP_NAME = "Trades";
    private static final String MODULE_DIRECTORY = moduleDirectory();
    private static final String INPUT_PATH = MODULE_DIRECTORY + "/hdfs-parquet-input";
    private static final String OUTPUT_PATH = MODULE_DIRECTORY + "/hdfs-parquet-output";

    private static final String INPUT_BUCKET_NAME = "jet-hdfs-parquet-input";
    private static final String OUTPUT_BUCKET_NAME = "jet-hdfs-parquet-output";

    public static void main(String[] args) throws Exception {
    	new MapJournalToHadoop().go();
    }

    private void go()  throws Exception {
        Path inputPath = new Path(INPUT_PATH);
        Path outputPath = new Path(OUTPUT_PATH);
        FileSystem.get(new Configuration()).delete(outputPath, true);
        try {
            HazelcastInstance hz = Hazelcast.bootstrappedInstance();
            initMapConfig(hz);
            JetService jet = hz.getJet();
            
            //Configuration jobHadoopConf = getHadoopJobConfWithKerberosAuthentication();
          
            JobConfig syncHadoopJetJobConfig = new JobConfig().setName("syncTradeMapToHadoop")
            		.setAutoScaling(true)
            		.setSuspendOnFailure(true);
            
            jet.newJobIfAbsent(buildSyncTradeMapToHadoopPipeline(getHadoopConf(inputPath, outputPath)), syncHadoopJetJobConfig);
            JobConfig tradeMapWriterJetJobConfig = new JobConfig().setName("tradeMapWriter")
            		.setAutoScaling(true)
            		.setSuspendOnFailure(true);
            jet.newJobIfAbsent(buildTradeMapWriterPipeline(), tradeMapWriterJetJobConfig);

            TimeUnit.SECONDS.sleep(10);
            long mapChangeOperationsCount = hz.getMap(MAP_NAME).getLocalMapStats().getPutOperationCount() + hz.getMap(MAP_NAME).getLocalMapStats().getSetOperationCount();
            System.out.println("Read " + mapChangeOperationsCount + " events from map journal.");
        } finally {
            Hazelcast.shutdownAll();
        }
	}
    
    private static Pipeline buildTradeMapWriterPipeline() {
         BatchSource<Trade> source = FileSources.files(Paths.get(MODULE_DIRECTORY+"/src/main/resources/data/trades").toAbsolutePath().toString())
                 .glob("*jsonl")
                 .format(FileFormat.json(Trade.class))
                 .build();

         Pipeline p = Pipeline.create();
         p.readFrom(source)
         	.map(trade -> new SimpleImmutableEntry<Long,Trade>(trade.getTime(), trade))
         	.writeTo(Sinks.map(MAP_NAME));
    	 return p;
    }
    
	private static Pipeline buildSyncTradeMapToHadoopPipeline(Configuration hadoopConfiguration) throws IOException {
        Pipeline p = Pipeline.create();
        p.readFrom(Sources.<Long, Trade>mapJournal(MAP_NAME, START_FROM_OLDEST))
         .withoutTimestamps()
         .map(Entry::getValue)
         .writeTo(HadoopSinks.outputFormat(hadoopConfiguration, trade -> null, trade -> trade));
         return p;
    }
	
    private static Config initMapConfig(HazelcastInstance hz) {
        Config cfg = new Config();
        // Add an event journal config for map which has custom capacity of 10_000
        // and time to live seconds as 10 seconds (default 0 which means infinite)
        hz.getConfig().getMapConfig(MAP_NAME)
           .setStatisticsEnabled(true)
           .getEventJournalConfig()
           .setEnabled(true)
           .setCapacity(10_000)
           .setTimeToLiveSeconds(10);
        hz.getConfig().getJetConfig().setResourceUploadEnabled(true);
        return cfg;
    }
    
    private static Configuration getHadoopConf(Path inputPath, Path outputPath) throws IOException {
    	Job job = Job.getInstance();
    	Configuration conf = job.getConfiguration();
    	//The following property is enough for a non-kerberized setup
        conf.set("fs.defaultFS", "localhost:9000");
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        //AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, Trade.SCHEMA$);
        AvroParquetInputFormat.setAvroReadSchema(job, Trade.SCHEMA$);
    	return conf;
    }
    
    private static  Configuration getHadoopConfWithKerberosAuthentication() throws IOException {
    	Job job = Job.getInstance();
    	Configuration conf = job.getConfiguration();
    	 
    	//The following property is enough for a non-kerberized setup
        //      conf.set("fs.defaultFS", "localhost:9000");

        //The location of krb5.conf file needs to be provided in the VM arguments for the JVM
        //-Djava.security.krb5.conf=/Users/jsherwin/hazelcast/myTestCluster/conf/krb5.conf
        //System.setProperty("java.security.krb5.conf","/Users/jsherwin/hazelcast/myTestCluster/conf/krb5.conf");

        // set kerberos host and realm
        System.setProperty("java.security.krb5.realm", "DRB.COM");
        System.setProperty("java.security.krb5.kdc", "192.168.33.10");
  	

        //Set properties to access a kerberized cluster
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");

        conf.set("fs.defaultFS", "hdfs://devha:8020");
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());

        // hack for running locally with fake DNS records
        // set this to true if overriding the host name in /etc/hosts
        conf.set("dfs.client.use.datanode.hostname", "true");

        // server principal
        // the kerberos principle that the namenode is using
        conf.set("dfs.namenode.kerberos.principal.pattern", "hduser/*@DRB.COM");

        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab("user@DRB.COM", "src/main/resources/user.keytab");
        
        job.setOutputFormatClass(AvroParquetOutputFormat.class);
        //AvroParquetOutputFormat.setOutputPath(job, outputPath);
        AvroParquetOutputFormat.setSchema(job, Trade.SCHEMA$);
        AvroParquetInputFormat.setAvroReadSchema(job, Trade.SCHEMA$);
    	return conf;
    }
     
    private static String moduleDirectory() {
        String resourcePath = MapJournalToHadoop.class.getClassLoader().getResource("").getPath();
        return Paths.get(resourcePath).getParent().getParent().toString();
    }

}