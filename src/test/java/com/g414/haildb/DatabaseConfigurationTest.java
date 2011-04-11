package com.g414.haildb;

import org.testng.annotations.Test;

@Test
public class DatabaseConfigurationTest {
    public void testBuilder() {
        DatabaseConfiguration c = new DatabaseConfiguration();
        c.setAdaptiveFlushingEnabled(c.isAdaptiveFlushingEnabled());
        c.setAdaptiveHashEnabled(c.isAdaptiveHashEnabled());
        c.setDoublewriteEnabled(c.isDoublewriteEnabled());
        c.setFilePerTableEnabled(c.isFilePerTableEnabled());
        c.setPageChecksumsEnabled(c.isPageChecksumsEnabled());
        c.setPrintVerboseLog(c.isPrintVerboseLog());
        c.setRollbackOnTimeoutEnabled(c.isRollbackOnTimeoutEnabled());
        c.setStatusFileEnabled(c.isStatusFileEnabled());
        c.setSysMallocEnabled(c.isSysMallocEnabled());

        c.setAdditionalMemPoolSize(c.getAdditionalMemPoolSize());
        c.setAutoextendIncrementSizePages(c.getAutoextendIncrementSizePages());
        c.setBufferPoolSize(c.getBufferPoolSize());
        c.setDatafilePath(c.getDatafilePath());
        c.setDataHomeDir(c.getDataHomeDir());
        c.setFileFormat(c.getFileFormat());
        c.setFlushLogAtTrxCommitMode(c.getFlushLogAtTrxCommitMode());
        c.setFlushMethod(c.getFlushMethod());
        c.setIoCapacityIOPS(c.getIoCapacityIOPS());
        c.setLockWaitTimeoutSeconds(c.getLockWaitTimeoutSeconds());
        c.setLogBufferSize(c.getLogBufferSize());
        c.setLogFileHomeDirectory(c.getLogFileHomeDirectory());
        c.setLogFilesInGroup(c.getLogFilesInGroup());
        c.setLogFileSizeMegabytes(c.getLogFileSize());
        c.setLruBlockAccessRecency(c.getLruBlockAccessRecency());
        c.setLruOldBlocksPct(c.getLruOldBlocksPct());
        c.setMaxDirtyPagesPct(c.getMaxDirtyPagesPct());
        c.setMaxPurgeLagSeconds(c.getMaxPurgeLagSeconds());
        c.setOpenFilesLimit(c.getOpenFilesLimit());
        c.setRecoveryMethod(c.getRecoveryMethod());
        c.setSyncSpinLoops(c.getSyncSpinLoops());
    }
}
