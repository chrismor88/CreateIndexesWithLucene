package index.types_from_relation;


import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import messages.Message;





public class ConsumerTypesFromRel extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private IndexWriter myIndexWriter;



	public ConsumerTypesFromRel(BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer,IndexWriter writer){
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
				String frequency = fieldsRecord[0];
				String predicate = fieldsRecord[1];
				String type1 = fieldsRecord[2];
				String type2 = fieldsRecord[3];


				//creazione del Document con i relativi campi d'interesse
				Document doc = new Document();


				Field predicateField = new TextField("predicate",predicate,Field.Store.YES);
				predicateField.setBoost(2.0f);

				Field type1Field= new TextField("subjectType",type1,Field.Store.YES);
				Field type2Field= new TextField("objectType",type2,Field.Store.YES);
				Field frequencyField = new StringField("frequency",frequency,Field.Store.YES);
				
				doc.add(frequencyField);
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
