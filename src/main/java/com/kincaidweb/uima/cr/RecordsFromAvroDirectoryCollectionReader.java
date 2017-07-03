package com.kincaidweb.uima.cr;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * UIMA Collection reader that reads records from all the Avro files in a directory. If a preProcessor is provided
 * it will be used to process the content of the ContentField in each record. Otherwise the value of the ContentField
 * is assumed to be a text string.
 *
 * Parameters:
 * <ul>
 *     <li>DirectoryName: full path to the directory containing the Avro files to be read.</li>
 *     <li>DocumentIdField: (not currently used)</li>
 *     <li>ContentField: the name of the field in the schema that holds the text of the document</li>
 * </ul>
 */
public class RecordsFromAvroDirectoryCollectionReader extends DirectoryCollectionReader {
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public static final String PARAM_DOCUMENT_ID_FIELD = "DocumentIdField";
    public static final String PARAM_CONTENT_FIELD = "ContentField";

    @ConfigurationParameter(name = PARAM_DOCUMENT_ID_FIELD)
    private String documentIdField;

    @ConfigurationParameter(name = PARAM_CONTENT_FIELD)
    private String contentField;

    private RecordTextPreProcessor preProcessor = null;

    private int recordsRead;
    private FileReader<GenericRecord> currentReader;

    @Override
    public void doInitialize(UimaContext context) {
        documentIdField = (String) getConfigParameterValue(PARAM_DOCUMENT_ID_FIELD);
        contentField = (String) getConfigParameterValue(PARAM_CONTENT_FIELD);
        currentReader = openReader(nextFile());
    }

    private FileReader<GenericRecord> openReader(String filename) {
        FileReader<GenericRecord> reader = null;

        try {
            reader = DataFileReader.openReader(new File(filename), new GenericDatumReader<>());
        } catch (IOException e) {
            logger.error("Could not open file " + filename, e);
        }

        return reader;
    }

    @Override
    public void getNext(JCas jcas) throws IOException, CollectionException {
        if (!currentReader.hasNext()) {
            currentReader.close();
            currentReader = openReader(nextFile());
        }

        GenericRecord nextRecord = currentReader.next();
        String documentId = String.valueOf(nextRecord.get(documentIdField));

        if (preProcessor != null) {
            preProcessor.preProcess(nextRecord, (ByteBuffer) nextRecord.get(contentField), jcas);
        } else {
            String content = String.valueOf(nextRecord.get(contentField));
            jcas.setDocumentText(content);
        }

        //DocumentID docId = new DocumentID(jcas);
        //docId.setDocumentID(documentId);
        //docId.addToIndexes();

        recordsRead++;
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        return currentReader.hasNext() || hasFiles();
    }

    @Override
    public Progress[] getProgress() {
        // TODO: implement
        return new Progress[] {
                new ProgressImpl(recordsRead, 0, Progress.ENTITIES)
        };
    }

    @Override
    public void close() throws IOException {
        currentReader.close();
    }

    public void setPreProcessor(RecordTextPreProcessor preProcessor) {
        this.preProcessor = preProcessor;
    }
}
