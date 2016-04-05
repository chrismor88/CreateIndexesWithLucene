package index.mapping_table;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;

import index.messages.Message;

public class ConsumerWikidMid extends Thread {

	private BlockingQueue<String> messageBuffer; //buffer in cui vengono trasmessi e prelevati le stringhe di tipo json
	private BlockingQueue<String> outputBuffer; //buffer per comunicare al produttore la terminazione dei consumatori
	private IndexWriter myIndexWriter;



	public ConsumerWikidMid(BlockingQueue<String> messageBuffer, BlockingQueue<String> responseBuffer,IndexWriter writer){
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

				String [] fieldsRecord = message.split(" ");
				String title = fieldsRecord[0];
				String mid = fieldsRecord[2];
		


				//creazione del Document con i relativi campi d'interesse
				Document doc = new Document();



				Field titleField= new TextField("title",title,Field.Store.YES);
				titleField.setBoost(2.0f);
				Field midField= new TextField("mid",mid,Field.Store.YES);
				midField.setBoost(2.0f);

				

				doc.add(titleField);
				doc.add(midField);
				System.out.println(title+" "+mid);

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