package org.sagebionetworks.bridge.udd.synapse;

import java.util.HashSet;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

/**
 * Contains info about columns for a particular Synapse table. In particular, the indices for file handle ID columns
 * and health codes.
 */
public class SynapseTableColumnInfo {
    private final Set<Integer> fileHandleColumnIndexSet;
    private final int healthCodeColumnIndex;

    /** Private constructor. To construct, use Builder. */
    private SynapseTableColumnInfo(Set<Integer> fileHandleColumnIndexSet, int healthCodeColumnIndex) {
        this.fileHandleColumnIndexSet = ImmutableSet.copyOf(fileHandleColumnIndexSet);
        this.healthCodeColumnIndex = healthCodeColumnIndex;
    }

    /** Set of column indices for columns that are file handle IDs. */
    public Set<Integer> getFileHandleColumnIndexSet() {
        return fileHandleColumnIndexSet;
    }

    /** Column index for health code. */
    public int getHealthCodeColumnIndex() {
        return healthCodeColumnIndex;
    }

    public static class Builder {
        private final Set<Integer> fileHandleColumnIndexSet = new HashSet<>();
        private Integer healthCodeColumnIndex;

        /** Adds zero or more file handle column indices. */
        public Builder addFileHandleColumnIndex(int... fileHandleColumnIndices) {
            for (int oneFileHandleColIdx : fileHandleColumnIndices) {
                fileHandleColumnIndexSet.add(oneFileHandleColIdx);
            }
            return this;
        }

        /** @see SynapseTableColumnInfo#getHealthCodeColumnIndex */
        public Builder withHealthCodeColumnIndex(int healthCodeColumnIndex) {
            this.healthCodeColumnIndex = healthCodeColumnIndex;
            return this;
        }

        /** Builds a SynapseTableColumnInfo and validates fields. */
        public SynapseTableColumnInfo build() {
            if (healthCodeColumnIndex == null) {
                throw new IllegalStateException("healthCodeColumnIndex must be specified");
            }

            // No need to validate fileHandleColumnIndexSet, since it's guaranteed to be non-null, can only contain
            // non-null entries, and is allowed to be empty.

            return new SynapseTableColumnInfo(fileHandleColumnIndexSet, healthCodeColumnIndex);
        }
    }
}
