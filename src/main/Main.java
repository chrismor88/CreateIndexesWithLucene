package main;

import java.io.FileNotFoundException;
import java.io.IOException;

import index.kb_table.InvertedIndexKB;
import index.mapping_table.InvertedIndexWikidMid;
import index.redirect.InvertedIndexRedirect;
import index.relations_types.InvertedIndexRT;

public class Main {

	public static void main(String[] args) {
		try {
			InvertedIndexKB.createInvertedIndex();
//			InvertedIndexWikidMid.createIndexWikidMid();
//			InvertedIndexRedirect.createIndexRedirect();
//			InvertedIndexRT.createInvertedIndex();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
