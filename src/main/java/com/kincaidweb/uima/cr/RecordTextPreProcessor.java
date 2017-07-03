package com.kincaidweb.uima.cr;

import org.apache.avro.generic.GenericRecord;
import org.apache.uima.jcas.JCas;

import java.nio.ByteBuffer;

/**
 * Interface used to define pre-processors for Avro GenericRecords to produce the document text. The pre-processor
 * is responsible for setting the document text in the JCas object and any other annotations needed.
 */
public interface RecordTextPreProcessor {

    public void preProcess(GenericRecord record, ByteBuffer content, JCas jcas);
}
