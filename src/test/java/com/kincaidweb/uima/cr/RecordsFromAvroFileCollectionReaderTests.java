package com.kincaidweb.uima.cr;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.testng.annotations.Test;

import java.net.URL;

import static org.apache.uima.fit.factory.CollectionReaderFactory.createReader;
import static org.testng.Assert.assertNotNull;

public class RecordsFromAvroFileCollectionReaderTests {

    @Test
    public void readerTest() throws Exception {
        URL testFile = ClassLoader.getSystemResource("part-00683.avro");

        RecordsFromAvroFileCollectionReader collectionReader = new RecordsFromAvroFileCollectionReader();
        collectionReader = (RecordsFromAvroFileCollectionReader) createReader(RecordsFromAvroFileCollectionReader.class,
                RecordsFromAvroFileCollectionReader.PARAM_INPUT_FILE_NAME, testFile.getPath(),
                RecordsFromAvroFileCollectionReader.PARAM_DOCUMENT_ID_FIELD, "mnRowKey",
                RecordsFromAvroFileCollectionReader.PARAM_CONTENT_FIELD, "mnText");

        collectionReader.initialize();

        while (collectionReader.hasNext()) {
            JCas jCas = JCasFactory.createJCas();
            collectionReader.getNext(jCas.getCas());
            String documentText = jCas.getDocumentText();
            assertNotNull(documentText);
        }
    }
}
