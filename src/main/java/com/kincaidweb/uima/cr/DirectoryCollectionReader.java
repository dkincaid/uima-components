package com.kincaidweb.uima.cr;

import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.fit.component.JCasCollectionReader_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.File;
import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Abstract class for use by collection readers which read from all files in a directory. This class requires
 * the following UIMA parameters to be set:
 *
 * Parameters:
 * <ul>
 *     <li>DirectoryName: full path to the Avro file to be read.</li>
 * </ul>
 */
public abstract class DirectoryCollectionReader extends JCasCollectionReader_ImplBase {
    private Logger logger = Logger.getLogger(this.getClass().getName());

    public static final String PARAM_DIRECTORY_NAME = "DirectoryName";

    @ConfigurationParameter(name = PARAM_DIRECTORY_NAME)
    private String directoryName;

    private Queue<String> filenames = new ConcurrentLinkedQueue<>();

    public abstract void doInitialize(UimaContext context);

    @Override
    public void initialize(UimaContext context) throws ResourceInitializationException {
        directoryName = (String) getConfigParameterValue(PARAM_DIRECTORY_NAME);
        setFileList(directoryName);
        doInitialize(context);
    }

    String nextFile() {
        return filenames.remove();
    }

    boolean hasFiles() {
        return !filenames.isEmpty();
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
}
