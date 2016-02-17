package util;

public class DocsCounter {
	private int counter;
	
	public DocsCounter() {
		counter = 0;
	}
	
	public synchronized void incrementCounter(){
		counter++;
	}
	
	public synchronized int getCounter(){
		return this.counter;
	}
}
