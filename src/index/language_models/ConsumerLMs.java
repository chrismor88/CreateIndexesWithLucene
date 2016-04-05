package index.language_models;


import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import index.messages.Message;
import index.util.DocsCounter;





public class ConsumerLMs extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private IndexWriter myIndexWriter;
	private DocsCounter docsCounter;



	public ConsumerLMs(BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer,IndexWriter writer, DocsCounter docsCounter){
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
				String rel = fieldsRecord[0];
				String pattern = fieldsRecord[1];
				String accuracy = fieldsRecord[5];
				
				Document doc = new Document();

				Field patternField = new TextField("pattern",pattern,Field.Store.YES);
				patternField.setBoost(2.0f);

				Field relField= new StringField("rel",rel,Field.Store.YES);
				Field accuracyField = new StringField("accuracy",accuracy,Field.Store.YES);

				doc.add(relField);
				doc.add(patternField);
				doc.add(accuracyField);

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
