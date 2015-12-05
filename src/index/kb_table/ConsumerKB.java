package index.kb_table;


import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import messages.Message;





public class ConsumerKB extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private IndexWriter myIndexWriter;



	public ConsumerKB(BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer,IndexWriter writer){
		this.messageBuffer = messageBuffer;
		this.outputBuffer = responseBuffer;
		this.myIndexWriter = writer;


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
				String predicate = fieldsRecord[2];
				String title1 = fieldsRecord[3];
				String title2 = fieldsRecord[4];
				String types1 = fieldsRecord[5];
				String types2 = fieldsRecord[6];


				//creazione del Document con i relativi campi d'interesse
				Document doc = new Document();



				Field mid1Field= new TextField("mid1",mid1,Field.Store.YES);
				mid1Field.setBoost(2.0f);
				Field mid2Field= new TextField("mid2",mid2,Field.Store.YES);
				mid2Field.setBoost(2.0f);

				Field predicateField = new StringField("predicate",predicate,Field.Store.YES);
				Field title1Field = new StringField("title1",title1,Field.Store.YES);
				Field title2Field = new StringField("title2",title2,Field.Store.YES);
				Field types1Field = new StringField("types1",types1,Field.Store.YES);
				Field types2Field = new StringField("types2",types2,Field.Store.YES);

				doc.add(mid1Field);
				doc.add(mid2Field);
				doc.add(predicateField);
				doc.add(title1Field);
				doc.add(title2Field);
				doc.add(types1Field);
				doc.add(types2Field);



				synchronized (myIndexWriter) {
					try {
						myIndexWriter.addDocument(doc);
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
