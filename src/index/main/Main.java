package index.main;

import java.io.FileNotFoundException;
import java.io.IOException;

import index.kb_table.InvertedIndexKB;
import index.language_models.InvertedIndexLMs;
import index.mapping_table.InvertedIndexWikidMid;
import index.relations_types.InvertedIndexRT;
import index.types_from_relation.InvertedIndexTypesFromRel;
import index.unrelated_pairs.InvertedIndexUnrelatedPairs;

public class Main {

	public static void main(String[] args) {
		try {
//			InvertedIndexLMs.createInvertedIndex();
			InvertedIndexKB.createInvertedIndex();
//			InvertedIndexWikidMid.createIndexWikidMid();
//			InvertedIndexRT.createInvertedIndex();
//			InvertedIndexTypesFromRel.createInvertedIndex();
//			InvertedIndexUnrelatedPairs.createInvertedIndex();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

}
