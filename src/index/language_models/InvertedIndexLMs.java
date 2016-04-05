package index.language_models;

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
import index.util.DocsCounter;

public class InvertedIndexLMs {

	static final String IndexPath = "C:\\Users\\Christian\\Documents\\Tesi\\lucene\\index_LMs";
	static final String LMsPath = "C:\\Users\\Christian\\Documents\\Tesi\\componenti\\full_LMs.tsv";
	


	public static void createInvertedIndex() throws FileNotFoundException, IOException {
		DocsCounter docsCounter = new DocsCounter();
		BlockingQueue<String> messageBuffer = new LinkedBlockingQueue<String>(1000);
		BlockingQueue<String> responseBuffer = new LinkedBlockingQueue<String>(10);
		int cores = Runtime.getRuntime().availableProcessors();

		ConsumerLMs[] consumers = new ConsumerLMs[cores];

		FileReader f = new FileReader(LMsPath);
		BufferedReader b = new BufferedReader(f);

		System.out.println("Creazione Indice inverso nella direcory: " +IndexPath + "'...");
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_47);

		Directory index = FSDirectory.open(new File((IndexPath)));
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_47,analyzer);
		config.setOpenMode(OpenMode.CREATE_OR_APPEND);
		config.setRAMBufferSizeMB(8192);

		Date start = new Date();

		IndexWriter writer = new IndexWriter(index, config);


		for(int i=0; i< consumers.length;i++){
			consumers[i] = new ConsumerLMs(messageBuffer, responseBuffer,writer,docsCounter);
			consumers[i].start();
		}


		String line ="";
		int counter = 0;
		while((line=b.readLine())!=null){
			try {
				if(counter>0){
					messageBuffer.put(line);
				}
				counter++;
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

		System.out.println("CONCLUSA. ");
		Date end = new Date();
		System.out.println(end.getTime() - start.getTime() + " total milliseconds");
		System.out.println((end.getTime() - start.getTime()) / 1000 +" secs");
	}
}