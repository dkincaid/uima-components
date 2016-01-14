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
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * UIMA Collection reader that reads records from all the Avro files in a directory.
 *
 * Parameters:
 * <ul>
 *     <li>DirectoryName: full path to the Avro file to be read.</li>
 *     <li>DocumentIdField: (not currently used)</li>
 *     <li>ContentField: the name of the field in the schema that holds the text of the document</li>
 * </ul>
 */
public class RecordsFromAvroDirectoryCollectionReader extends JCasCollectionReader_ImplBase {
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public static final String PARAM_DIRECTORY_NAME = "DirectoryName";
    public static final String PARAM_DOCUMENT_ID_FIELD = "DocumentIdField";
    public static final String PARAM_CONTENT_FIELD = "ContentField";

    @ConfigurationParameter(name = PARAM_DIRECTORY_NAME)
    private String directoryName;

    @ConfigurationParameter(name = PARAM_DOCUMENT_ID_FIELD)
    private String documentIdField;

    @ConfigurationParameter(name = PARAM_CONTENT_FIELD)
    private String contentField;

    private Queue<String> filenames = new ConcurrentLinkedQueue<>();
    private int recordsRead;
    private FileReader<GenericRecord> currentReader;

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        directoryName = (String) getConfigParameterValue(PARAM_DIRECTORY_NAME);
        documentIdField = (String) getConfigParameterValue(PARAM_DOCUMENT_ID_FIELD);
        contentField = (String) getConfigParameterValue(PARAM_CONTENT_FIELD);

        setFileList(directoryName);
        currentReader = openReader(filenames.remove());
    }

    private void setFileList(String directory) {
        File dir = new File(directory);
        File[] files = dir.listFiles();

        if (files != null) {
            for (File file: files) {
                if (file.isFile()) {
                    try {
                        filenames.offer(file.getCanonicalPath());
                    } catch (IOException e) {
                        logger.error("Unable to get the canonical path for " + file, e);
                    }
                } else if (file.isDirectory()) {
                    setFileList(file.getAbsolutePath());
                }
            }
        } else {
            logger.warn("No files found in the directory [" + directory + "]!");
        }
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
            currentReader = openReader(filenames.remove());
        }

        GenericRecord nextRecord = currentReader.next();
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
        return currentReader.hasNext() || !filenames.isEmpty();
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
}
