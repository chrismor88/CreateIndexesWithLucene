package index.relations_types;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import index.messages.Message;

public class InvertedIndexRT {

	
	static final String IndexPath = "C:\\Users\\Christian\\Documents\\Tesi\\lucene\\index_schema_complete";
	static final String RTPath = "C:\\Users\\Christian\\Documents\\Tesi\\componenti\\schema_complete.tsv";


	public static void createInvertedIndex() throws FileNotFoundException, IOException {
		BlockingQueue<String> messageBuffer = new LinkedBlockingQueue<String>(1000);
		BlockingQueue<String> responseBuffer = new LinkedBlockingQueue<String>(10);
		int cores = Runtime.getRuntime().availableProcessors();
		
		ConsumerRT[] consumers = new ConsumerRT[cores];
		
		FileReader f = new FileReader(RTPath);
		BufferedReader b = new BufferedReader(f);

		System.out.println("Creazione Indice inverso nella direcory: " +IndexPath + "'...");
		Analyzer analyzer = new KeywordAnalyzer();

		Directory index = FSDirectory.open(new File((IndexPath)));
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47,analyzer);
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);

		Date start = new Date();
		
		IndexWriter writer = new IndexWriter(index, config);
		
		
		for(int i=0; i< consumers.length;i++){
			consumers[i] = new ConsumerRT(messageBuffer, responseBuffer,writer);
			consumers[i].start();
		}
		

		
		String line ="";
		while((line=b.readLine())!=null){
			try {
				messageBuffer.put(line);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
		}
		try {
			messageBuffer.put(Message.FINISHED_PRODUCER);
			int counterConsumerFinished = 0;
			while(counterConsumerFinished<cores){
				String message = responseBuffer.take();
				if(message.equals(Message.FINISHED_CONSUMER))
					counterConsumerFinished++;
			}
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		
		writer.close();
		b.close();
		f.close();

		Date end = new Date();
		System.out.println(end.getTime() - start.getTime() + " total milliseconds");
		System.out.println("CONCLUSA");
	}
}