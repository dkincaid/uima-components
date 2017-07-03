package com.kincaidweb.uima.cr;

import org.apache.avro.generic.GenericRecord;

/**
 * Interface used to define pre-processors for Avro GenericRecords to produce the document text.
 */
public interface RecordTextPreProcessor {

    public String preProcess(GenericRecord record, byte[] content);
}
