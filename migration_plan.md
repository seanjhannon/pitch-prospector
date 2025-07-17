# SQL Migration Plan for Pitch Prospector

## Current State
- File-based storage (Parquet files)
- ~200MB of indexed data
- Auto-refresh writes to filesystem

## Target State
- PostgreSQL/SQLite database
- Efficient indexed queries
- Cloud-friendly deployment

## Migration Strategy

### Phase 1: Database Setup
1. **Choose Database**: PostgreSQL (production) or SQLite (development)
2. **Schema Design**: 
   ```sql
   -- Core tables
   CREATE TABLE atbats (
       id SERIAL PRIMARY KEY,
       game_pk INTEGER,
       at_bat_number INTEGER,
       game_date DATE,
       batter INTEGER,
       pitcher INTEGER,
       inning INTEGER,
       pitch_sequence_hash VARCHAR(40),
       UNIQUE(game_pk, at_bat_number)
   );
   
   CREATE TABLE pitch_sequences (
       id SERIAL PRIMARY KEY,
       atbat_id INTEGER REFERENCES atbats(id),
       pitch_order INTEGER,
       pitch_type VARCHAR(10),
       description VARCHAR(50),
       release_speed DECIMAL(4,1),
       zone INTEGER
   );
   
   CREATE INDEX idx_pitch_sequence_hash ON atbats(pitch_sequence_hash);
   CREATE INDEX idx_game_date ON atbats(game_date);
   ```

### Phase 2: Data Migration
1. **Create migration script** to convert Parquet → SQL
2. **Test with subset** of data first
3. **Full migration** with rollback plan

### Phase 3: App Updates
1. **Replace file loading** with database queries
2. **Update search logic** to use SQL
3. **Add connection pooling**
4. **Implement caching layer**

### Phase 4: Cloud Deployment
1. **Database hosting** (Supabase, Railway, etc.)
2. **Environment variables** for connections
3. **Connection pooling** in Streamlit Cloud

## Benefits of SQL Migration
- ✅ **10-100x faster searches** (indexed queries)
- ✅ **Reduced memory usage** (no full DataFrame loading)
- ✅ **Cloud-friendly** (no filesystem writes)
- ✅ **Scalable** (handles growing data)
- ✅ **ACID compliance** (data integrity)

## Implementation Priority
1. **High Impact**: Database schema and migration
2. **Medium Impact**: App query updates
3. **Low Impact**: Connection pooling and caching 