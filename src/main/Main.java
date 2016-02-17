package main;

import java.io.FileNotFoundException;
import java.io.IOException;

import index.kb_table.InvertedIndexKB;
import index.mapping_table.InvertedIndexWikidMid;
import index.redirect.InvertedIndexRedirect;
import index.relations_types.InvertedIndexRT;
import index.types_from_relation.InvertedIndexTypesFromRel;
import index.unrelated_pairs.InvertedIndexUnrelatedPairs;

public class Main {

	public static void main(String[] args) {
		try {
//			InvertedIndexKB.createInvertedIndex();
//			InvertedIndexWikidMid.createIndexWikidMid();
//			InvertedIndexRedirect.createIndexRedirect();
//			InvertedIndexRT.createInvertedIndex();
//			InvertedIndexTypesFromRel.createInvertedIndex();
			InvertedIndexUnrelatedPairs.createInvertedIndex();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
