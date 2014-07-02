package com.indeed.proctor.webapp.db;

import com.google.common.base.CharMatcher;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.indeed.util.varexport.VarExporter;
import com.indeed.proctor.store.*;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;

public class GitProctorStoreFactory implements StoreFactory {
    private static final Logger LOGGER = Logger.getLogger(GitProctorStoreFactory.class);

    final ScheduledExecutorService executor;

    private String gitUrl;
    private String gitUsername;
    private String gitPassword;

    /* The root directory into which we should put the "qa-matrices" or "trunk-matrices"
     * If not set - a the temp directory will be used
     * */
    File tempRoot;

    private final File implicitTempRoot;

    public GitProctorStoreFactory(final ScheduledExecutorService executor, final String gitUrl, final String gitUsername, final String gitPassword) throws IOException, ConfigurationException {
        this.executor = executor;
        this.gitUrl = gitUrl;
        this.gitUsername = gitUsername;
        this.gitPassword = gitPassword;
        this.implicitTempRoot = identifyImplicitTempRoot();
    }

    public ProctorStore getTrunkStore() {
        return createStore("/trunk/matrices");
    }

    public ProctorStore getQaStore() {
        return createStore("/branches/deploy/qa/matrices");
    }

    public ProctorStore getProductionStore() {
        return createStore("/branches/deploy/production/matrices");
    }
    
    public ProctorStore createStore(final String relativePath) {
        final File tempDirectory = createTempDirectoryForPath(relativePath);

        Preconditions.checkArgument(!CharMatcher.WHITESPACE.matchesAllOf(Strings.nullToEmpty(gitUrl)), "git.url property cannot be empty");
        // TODO (parker) 9/13/12 - sanity check that path + relative path make a valid url
        final String fullPath = gitUrl + relativePath;

        // final gitWorkspaceProviderImpl provider = new gitWorkspaceProviderImpl(tempDirectory, tempDirCleanupAgeMillis);
        // final gitPersisterCoreImpl gitcore = new gitPersisterCoreImpl(fullPath, gitUsername, gitPassword, provider, true /* shutdown provider */);
        
        try {
            final GitProctor store = new GitProctor(gitUrl, gitUsername, gitPassword);
            final VarExporter exporter = VarExporter.forNamespace(GitProctor.class.getSimpleName()).includeInGlobal();
            final String prefix = relativePath.substring(1).replace('/', '-');
            exporter.export(store, prefix + "-");

            return store; //TODO FIX
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("createStore function in GitProctorStoreFactory - this should not be shown\n\n");
        return null;
    }

    private File createTempDirectoryForPath(final String relativePath) {
        // replace "/" with "-" omit first "/" but omitEmptyStrings
        final String dirName = CharMatcher.is(File.separatorChar).trimAndCollapseFrom(relativePath, '-');
        final File parent = tempRoot != null ? tempRoot : implicitTempRoot;
        final File temp = new File(parent, dirName);
        if(temp.exists()) {
           if(!temp.isDirectory()) {
               throw new IllegalStateException(temp + " exists but is not a directory");
           }
        } else {
            if(!temp.mkdir()) {
                throw new IllegalStateException("Could not create directory : " + temp);
            }
        }
        return temp;
    }

    /**
     * Identify the root-directory for TempFiles
     * @return
     */
    private File identifyImplicitTempRoot() throws IOException {
        final File tempFile = File.createTempFile("implicit", GitProctorStoreFactory.class.getSimpleName());

        tempFile.delete();
        return tempFile.getParentFile();
    }

    public File getTempRoot() {
        return tempRoot;
    }

    public void setTempRoot(File tempRoot) {
        this.tempRoot = tempRoot;
    }
}
