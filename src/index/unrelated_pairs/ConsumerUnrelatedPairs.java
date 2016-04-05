package index.unrelated_pairs;


import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import index.messages.Message;
import index.util.DocsCounter;





public class ConsumerUnrelatedPairs extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private IndexWriter myIndexWriter;
	private DocsCounter docsCounter;



	public ConsumerUnrelatedPairs(BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer,IndexWriter writer, DocsCounter docsCounter){
		this.messageBuffer = messageBuffer;
		this.outputBuffer = responseBuffer;
		this.myIndexWriter = writer;
		this.docsCounter = docsCounter;


	}



	@Override
	public void run() {
		super.run();
		while(true){
			String message = "";
			try {
				message = messageBuffer.take();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}


			if(message.equals(Message.FINISHED_PRODUCER)){
				break;
			}
			else{

				String [] fieldsRecord = message.split("\t");
				String mid1 = fieldsRecord[0];
				String mid2 = fieldsRecord[1];
				String patterns = fieldsRecord[2];


				//creazione del Document con i relativi campi d'interesse
				Document doc = new Document();


				Field mid1Field= new TextField("mid1",mid1,Field.Store.YES);
				mid1Field.setBoost(2.0f);
				Field mid2Field= new TextField("mid2",mid2,Field.Store.YES);
				mid2Field.setBoost(2.0f);

				Field patternField = new StringField("pattern",patterns,Field.Store.YES);
		

				doc.add(mid1Field);
				doc.add(mid2Field);
				doc.add(patternField);

				synchronized (myIndexWriter) {
					try {
						myIndexWriter.addDocument(doc);
						this.docsCounter.incrementCounter();
						System.out.println(this.docsCounter.getCounter());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}

		}



		try {
			messageBuffer.put(Message.FINISHED_PRODUCER);
			outputBuffer.put(Message.FINISHED_CONSUMER);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}


		this.interrupt();

	}



}
