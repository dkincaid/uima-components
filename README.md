# UIMA Components

[![Join the chat at https://gitter.im/dkincaid/uima-components](https://badges.gitter.im/dkincaid/uima-components.svg)](https://gitter.im/dkincaid/uima-components?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
Components for use with UIMA

![Travis Build Status](https://travis-ci.org/dkincaid/uima-components.svg?branch=develop)

## Components
### AvroFileWriterCasConsumer
- In package com.kincaidweb.uima.cc

A UIMA AnalysisEngine that serializes the CAS to an Avro file. Each instance of the class writes
batches to files prefixed with the provided file name followed by a "-" and a incrementing counter.

#### Parameters
- FileName: file name to write the records to
- DocumentIdField: the Avro field name to use for the document id field
- CasField: the Avro field name to use for the XMI serialized CAS
- Codec: the compression codec to use when writing records

### RecordsFromAvroFileCollectionReader
- In package com.kincaidweb.uima.cr

A UIMA Collection Reader that reads documents from an Avro file. 

#### Parameters
- InputFileName: name of the file to read documents from
- DocumentIdField: the Avro field name that contains the document id
- ContentField: the Avro field name that contains the text content of the document
