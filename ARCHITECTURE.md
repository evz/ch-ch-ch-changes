# Architecture Overview

## ETL Pipeline

The system implements a [Type 2 SCD (Slowly Changing
Dimension)](https://en.wikipedia.org/wiki/Slowly_changing_dimension) pattern to
track changes in Chicago's crime dataset. The interesting parts are in how it
handles the scale and the specific domain problems.

### Processing Flow

1. **Data Acquisition** - Downloads 8M+ records (~1.8GB) from Chicago Data Portal
2. **Staging** - Bulk load using PostgreSQL COPY 
3. **Deduplication** - Window functions to handle multiple records with same crime ID
4. **Change Detection** - Field-by-field comparison to identify modified records
5. **Versioning** - Temporal table updates maintaining full history
6. **Deletion Tracking** - Soft deletes for records removed from source

### Implementation Notes

**Deduplication Strategy**: Uses `RANK() OVER(PARTITION BY id ORDER BY line_num
DESC)` to handle the fact that Chicago's data sometimes contains multiple
entries for the same crime ID. Takes the last occurrence as canonical.

**Change Detection**: Explicit field comparison rather than hashing because the
domain requires knowing *what* changed, not just *that* something changed.
Particularly important for tracking when crimes move between FBI
index/non-index classifications.

**Materialized Views**: The `changed_records` view pre-computes which crime IDs
have multiple versions. This avoids expensive GROUP BY queries on 8M+ rows when
browsing changes.

**Reference Data**: Maintains separate pipeline for IUCR crime classification
codes since these can change independently and affect how existing crimes are
categorized.

### Domain-Specific Considerations

The system tracks changes that could indicate various issues with public crime
data integrity:
- Arrest status modifications after initial reporting
- Crime type reclassifications, particularly index ↔ non-index changes that
  affect FBI reporting statistics
- Retroactive location or date corrections
- Records that disappear entirely from the dataset

Since the Chicago Data Portal doesn't maintain historical snapshots, this
creates an independent record of how the data evolves over time. The patterns
of changes could reveal legitimate corrections, data quality issues, or
potentially other factors affecting reported crime statistics.
