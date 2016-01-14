package com.kincaidweb.uima.cc;

import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.jcas.JCas;
import org.xml.sax.SAXException;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * CAS Consumer that writes all the CAS objects to an Avro file
 */
public class AvroFileWriterCasConsumer extends JCasAnnotator_ImplBase {
    private static final Logger logger = Logger.getLogger(AvroFileWriterCasConsumer.class);

    public static final String PARAM_FILENAME = "FileName";
    public static final String PARAM_DOCUMENT_ID_FIELD = "DocumentIdField";
    public static final String PARAM_CAS_FIELD = "CasField";
    public static final String PARAM_CODEC = "Codec";

    @ConfigurationParameter(name = PARAM_FILENAME)
    private String fileName;

    @ConfigurationParameter(name = PARAM_DOCUMENT_ID_FIELD, defaultValue = "documentId")
    private String documentIdField;

    @ConfigurationParameter(name = PARAM_CAS_FIELD, defaultValue = "cas")
    private String casField;

    @ConfigurationParameter(name = PARAM_CODEC, defaultValue = "null")
    private String codec;

    private Schema schema;
    private DataFileWriter<GenericRecord> dataFileWriter;
    private DatumWriter<GenericRecord> datumWriter;

    private int batchCounter = 1;

    @Override
    public void initialize(UimaContext context) {
        documentIdField = (String) context.getConfigParameterValue(PARAM_DOCUMENT_ID_FIELD);
        casField = (String) context.getConfigParameterValue(PARAM_CAS_FIELD);
        fileName = (String) context.getConfigParameterValue(PARAM_FILENAME);
        codec = (String) context.getConfigParameterValue(PARAM_CODEC);

        String schemaJson = "{\"namespace\": \"com.idexx.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"CasAvro\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"" + documentIdField + "\", \"type\": \"string\"},\n" +
                "     {\"name\": \"" + casField + "\",  \"type\": \"string\"}\n" +
                " ]\n" +
                "}";

        schema = new Schema.Parser().parse(schemaJson);
        datumWriter = new GenericDatumWriter<>(schema);
        newDataFileWriter();
    }

    @Override
    public void process(JCas jCas) {
        String docName = "";
        //docName = DocumentIDAnnotationUtil.getDocumentID(jCas);
        //if (docName == null)
        //    docName = "";

        GenericRecord record = new GenericData.Record(schema);
        record.put(documentIdField, docName);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            XmiCasSerializer.serialize(jCas.getCas(), jCas.getTypeSystem(), baos);

            // Can't get this to work so that it can be read back in through the CPE
            //Serialization.serializeWithCompression(jCas.getCas(), baos, jCas.getCas().getTypeSystem());

            record.put(casField, new String(baos.toByteArray()));
            dataFileWriter.append(record);
        } catch (SAXException e) {
            logger.error("Error serializing CAS record for document id [" + docName + "]!" + e.getMessage());
        } catch (IOException e) {
            logger.error("Error writing CAS record for document id [" + docName + "]!" + e.getMessage());
        }
    }

    @Override
    public void batchProcessComplete() {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            logger.error("Error closing the Avro file writer!", e);
        }

        batchCounter++;
        newDataFileWriter();
    }

    @Override
    public void collectionProcessComplete() {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            logger.error("Error closing the Avro file writer!", e);
        }
    }

    private void newDataFileWriter() {
        File file = new File(fileName + "-" + batchCounter + ".avro");
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.fromString(codec));
        try {
            dataFileWriter.create(schema, file);
        } catch (IOException e) {
            logger.error("Failed to create the Avro file " + fileName, e);
        }
    }
}
