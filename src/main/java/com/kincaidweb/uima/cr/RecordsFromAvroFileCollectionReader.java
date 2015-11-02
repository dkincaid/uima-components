package com.kincaidweb.uima.cr;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

import java.io.File;
import java.io.IOException;

/**
 * UIMA Collection reader that reads records from an Avro file.
 *
 * Parameters:
 * <ul>
 *     <li>InputFileName: full path to the Avro file to be read.</li>
 *     <li>DocumentIdField: (not currently used)</li>
 *     <li>ContentField: the name of the field in the schema that holds the text of the document</li>
 * </ul>
 */
public class RecordsFromAvroFileCollectionReader extends JCasCollectionReader_ImplBase {
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public static final String PARAM_INPUT_FILE_NAME = "InputFileName";
    public static final String PARAM_DOCUMENT_ID_FIELD = "DocumentIdField";
    public static final String PARAM_CONTENT_FIELD = "ContentField";

    @ConfigurationParameter(name = PARAM_INPUT_FILE_NAME)
    private String inputFileName;

    @ConfigurationParameter(name = PARAM_DOCUMENT_ID_FIELD)
    private String documentIdField;

    @ConfigurationParameter(name = PARAM_CONTENT_FIELD)
    private String contentField;

    private FileReader<GenericRecord> reader;
    private int recordsRead;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        String inputFileName = (String) getConfigParameterValue(PARAM_INPUT_FILE_NAME);
        documentIdField = (String) getConfigParameterValue(PARAM_DOCUMENT_ID_FIELD);
        contentField = (String) getConfigParameterValue(PARAM_CONTENT_FIELD);

        try {
            reader = DataFileReader.openReader(new File(inputFileName), new GenericDatumReader<GenericRecord>());
        } catch (IOException e) {
            logger.error("Could not open file " + inputFileName, e);
        }
    }

    @Override
    public void getNext(JCas jcas) throws IOException, CollectionException {
            GenericRecord nextRecord = reader.next();
            String documentId = String.valueOf(nextRecord.get(documentIdField));
            String content = String.valueOf(nextRecord.get(contentField));

            jcas.setDocumentText(content);
            //DocumentID docId = new DocumentID(jcas);
            //docId.setDocumentID(documentId);
            //docId.addToIndexes();

            recordsRead++;
    }

    @Override
    public boolean hasNext() throws IOException, CollectionException {
        return reader.hasNext();
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
        reader.close();
    }
}
