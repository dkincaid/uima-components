package com.kincaidweb.uima.cr;

import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.testng.annotations.Test;

import java.net.URL;

import static org.apache.uima.fit.factory.CollectionReaderFactory.createReader;
import static org.testng.Assert.assertNotNull;

public class RecordsFromParquetDirectoryCollectionReaderTest {

    @Test
    public void readerTest() throws Exception {
        URL testFile = ClassLoader.getSystemResource("parquet/");

        RecordsFromParquetDirectoryCollectionReader collectionReader = new RecordsFromParquetDirectoryCollectionReader();
        collectionReader = (RecordsFromParquetDirectoryCollectionReader) createReader(RecordsFromParquetDirectoryCollectionReader.class,
                RecordsFromParquetDirectoryCollectionReader.PARAM_DIRECTORY_NAME, testFile.getPath(),
                RecordsFromParquetDirectoryCollectionReader.PARAM_DOCUMENT_ID_FIELD, "rowKey",
                RecordsFromParquetDirectoryCollectionReader.PARAM_CONTENT_FIELD, "content");

        collectionReader.initialize();

        while (collectionReader.hasNext()) {
            JCas jCas = JCasFactory.createJCas();
            collectionReader.getNext(jCas.getCas());
            String documentText = jCas.getDocumentText();
            assertNotNull(documentText);
        }
    }
}
