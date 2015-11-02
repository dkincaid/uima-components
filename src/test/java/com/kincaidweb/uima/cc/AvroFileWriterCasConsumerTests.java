package com.kincaidweb.uima.cc;

import com.kincaidweb.uima.cr.RecordsFromAvroFileCollectionReader;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.net.URL;

import static org.apache.uima.fit.factory.AnalysisEngineFactory.createEngine;
import static org.apache.uima.fit.factory.CollectionReaderFactory.createReader;
import static org.testng.Assert.assertNotNull;

public class AvroFileWriterCasConsumerTests {

    @Test
    public void serializationTest() throws Exception {
        URL testFile = ClassLoader.getSystemResource("part-00683.avro");

        AnalysisEngine avroFileWriterCasConsumer = createEngine(AvroFileWriterCasConsumer.class,
                AvroFileWriterCasConsumer.PARAM_FILENAME, "/tmp/test-ser.avro",
                AvroFileWriterCasConsumer.PARAM_DOCUMENT_ID_FIELD, "documentId",
                AvroFileWriterCasConsumer.PARAM_CAS_FIELD, "cas");

        RecordsFromAvroFileCollectionReader collectionReader = (RecordsFromAvroFileCollectionReader) createReader(RecordsFromAvroFileCollectionReader.class,
                RecordsFromAvroFileCollectionReader.PARAM_INPUT_FILE_NAME, testFile.getPath(),
                RecordsFromAvroFileCollectionReader.PARAM_DOCUMENT_ID_FIELD, "mnRowKey",
                RecordsFromAvroFileCollectionReader.PARAM_CONTENT_FIELD, "mnText");

        while (collectionReader.hasNext()) {
            JCas jCas = JCasFactory.createJCas();
            collectionReader.getNext(jCas.getCas());
            avroFileWriterCasConsumer.process(jCas);
        }

        avroFileWriterCasConsumer.collectionProcessComplete();

        JCas newJCas = JCasFactory.createJCas();
        CAS newCas = newJCas.getCas();

        File file = new File("/tmp/test-ser.avro-1.avro");
        //File file = new File("/tmp/idexx-notes-1.avro");

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader);
        GenericRecord casRecord = null;

        while (dataFileReader.hasNext()) {
            casRecord = dataFileReader.next(casRecord);
            byte[] bytes = casRecord.get("cas").toString().getBytes();
            XmiCasDeserializer.deserialize(new ByteArrayInputStream(bytes), newCas);
            assertNotNull(newCas.getDocumentText());
        }

        dataFileReader.close();
    }
}
