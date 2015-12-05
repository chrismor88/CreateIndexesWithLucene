package index.relations_types;


import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import messages.Message;





public class ConsumerRT extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private IndexWriter myIndexWriter;



	public ConsumerRT(BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer,IndexWriter writer){
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
				String type1 = fieldsRecord[0];
				String predicate = fieldsRecord[1];
				String type2 = fieldsRecord[2];
		


				//creazione del Document con i relativi campi d'interesse
				Document doc = new Document();



				Field type1Field= new TextField("type1",type1,Field.Store.YES);
				type1Field.setBoost(2.0f);
				Field type2Field= new TextField("type2",type2,Field.Store.YES);
				type2Field.setBoost(2.0f);

				Field predicateField = new StringField("predicate",predicate,Field.Store.YES);


				doc.add(type1Field);
				doc.add(type2Field);
				doc.add(predicateField);

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
