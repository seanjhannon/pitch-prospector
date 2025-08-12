# 🪳 CockroachDB Migration Guide for Pitch Prospector

This guide will help you migrate your Pitch Prospector application from Supabase PostgreSQL to CockroachDB, enabling you to store much more data.

## 🎯 Migration Overview

**Why CockroachDB?**
- **Unlimited storage** - No more 0.4GB limits
- **Global distribution** - Better performance for users worldwide
- **PostgreSQL compatibility** - Minimal code changes required
- **Enterprise-grade reliability** - Built for production workloads

## 📋 Prerequisites

1. **CockroachDB cluster** - Already set up at `pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud`
2. **CA certificate** - Downloaded to `~/.postgresql/root.crt`
3. **Connection details** - Available in `cockroach.txt`

## 🔧 Step 1: Update Dependencies

Add the required packages to your `pyproject.toml`:

```toml
dependencies = [
    # ... existing dependencies ...
    "psycopg[binary]>=3.1.0,<4.0.0",
    "psycopg-pool>=1.0.0,<2.0.0"
]
```

Install the new dependencies:
```bash
poetry install
```

## 🔐 Step 2: Update Streamlit Secrets

Update your `.streamlit/secrets.toml` file to include CockroachDB configuration:

```toml
# .streamlit/secrets.toml

[connections.supabase]
SUPABASE_URL = "https://xjjwtmcoklsqosxkexqw.supabase.co"
SUPABASE_KEY = "sb_publishable_dcLQbuoAg9rmrTBB6MHYNg__ufJZq3e"

# Add CockroachDB configuration
[crdb]
dsn = "postgresql://sean:_GS_iQHq4ZjjvwA4-VBqcQ@pitches-8229.jxf.gcp-us-west2.cockroachlabs.cloud:26257/defaultdb?sslmode=verify-full"
ca_cert_b64 = "LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUZhekNDQTFPZ0F3SUJBZ0lSQUlJUXo3RFNRT05aUkdQZ3UyT0Npd0F3RFFZSktvWklo
dmNOQVFFTEJRQXcKVHpFTE1Ba0dBMVVFQmhNQ1ZWTXhLVEFuQmdOVkJBb1RJRWx1ZEdWeWJtVjBJRk5sWTNWeWFYUjVJRkpsYzJWaApjbU5v
SUVkeWIzVndNUlV3RXdZRFZRUURFd3hKVTFKSElGSnZiM1FnV0RFd0hoY05NVFV3TmpBME1URXdORE00CldoY05NelV3TmpBME1URXdORE00
V2pCUE1Rc3dDUVlEVlFRR0V3SlZVekVwTUNjR0ExVUVDaE1nU1c1MFpYSnUKWlhRZ1UyVmpkWEpwZEhrZ1VtVnpaV0Z5WTJnZ1IzSnZkWEF4
RlRBVEJnTlZCQU1UREVsVFVrY2dVbTl2ZENCWQpNVENDQWlJd0RRWUpLb1pJaHZjTkFRRUJCUUFEZ2dJUEFEQ0NBZ29DZ2dJQkFLM29KSFAw
RkRmem01NHJWeWdjCmg3N2N0OTg0a0l4dVBPWlhvSGozZGNLaS92VnFidllBVHlqYjNtaUdiRVNUdHJGai9SUVNhNzhmMHVveG15RisKMFRN
OHVrajEzWG5mczdqL0V2RWhta3ZCaW9aeGFVcG1abXlQZmp4d3Y2MHBJZ2J6NU1EbWdLN2lTNCszbVg2VQpBNS9UUjVkOG1VZ2pVK2c0cms4
S2I0TXUwVWxYaklCMHR0b3YwRGlOZXdOd0lSdDE4akE4K28rdTNkcGpxK3NXClQ4S09FVXQrend2by83VjNMdlN5ZTByZ1RCSWxESENOQXlt
ZzRWTWs3QlBaN2htL0VMTktqRCtKbzJGUjNxeUgKQjVUMFkzSHNMdUp2VzVpQjRZbGNOSGxzZHU4N2tHSjU1dHVrbWk4bXhkQVE0UTdlMlJD
T0Z2dTM5NmozeCtVQwpCNWlQTmdpVjUrSTNsZzAyZFo3N0RuS3hIWnU4QS9sSkJkaUIzUVcwS3RaQjZhd0JkcFVLRDlqZjFiMFNIelV2CktC
ZHMwcGpCcUFsa2QyNUhON3JPckZsZWFKMS9jdGFKeFFaQktUNVpQdDBtOVNUSkVhZGFvMHhBSDBhaG1iV24KT2xGdWhqdWVmWEtuRWdWNFdl
MCtVWGdWQ3dPUGpkQXZCYkkrZTBvY1MzTUZFdnpHNnVCUUUzeERrM1N6eW5UbgpqaDhCQ05BdzFGdHhOclFIdXNFd01GeEl0NEk3bUtaOVlJ
cWlveW1DekxxOWd3UWJvb01EUWFIV0JmRWJ3cmJ3CnFIeUdPMGFvU0NxSTNIYWFkcjhmYXFVOUdZL3JPUE5rM3NnckRRb28vL2ZiNGhWQzFD
TFFKMTNoZWY0WTUzQ0kKclU3bTJZczZ4dDBuVVc3L3ZHVDFNME5QQWdNQkFBR2pRakJBTUE0R0ExVWREd0VCL3dRRUF3SUJCakFQQmdOVgpI
Uk1CQWY4RUJUQURBUUgvTUIwR0ExVWREZ1FXQkJSNXRGbm1lN2JsNUFGemdBaUl5QnBZOXVtYmJqQU5CZ2txCmhraUc5dzBCQVFzRkFBT0NB
Z0VBVlI5WXFieXlxRkRRRExIWUdta2dKeWtJckdGMVhJcHUrSUxsYVMvVjlsWkwKdWJoekVGblRJWmQrNTB4eCs3TFNZSzA1cUF2cUZ5Rldo
ZkZRRGxucnp1Qlo2YnJKRmUrR25ZK0VnUGJrNlpHUQozQmViWWh0RjhHYVYwbnh2d3VvNzd4L1B5OWF1Si9HcHNNaXUvWDErbXZvaUJPdi8y
WC9xa1NzaXNSY09qL0tLCk5GdFkyUHdCeVZTNXVDYk1pb2d6aVV3dGhEeUMzKzZXVndXNkxMdjN4TGZIVGp1Q3ZqSElJbk56a3RIQ2dLUTUK
T1JBekk0Sk1QSitHc2xXWUhiNHBob3dpbTU3aWF6dFhPb0p3VGR3Sng0bkxDZ2ROYk9oZGpzbnZ6cXZIdTdVcgpUa1hXU3RBbXpPVnl5Z2hx
cFpYakZhSDNwTzNKTEYrbCsvK3NLQUl1dnRkN3UrTnhlNUFXMHdkZVJsTjhOd2RDCmpOUEVscHpWbWJVcTRKVWFnRWl1VERrSHpzeEhwRktW
SzdxNCs2M1NNMU45NVIxTmJkV2hzY2RDYitaQUp6VmMKb3lpM0I0M25qVE9RNXlPZisxQ2NlV3hHMWJRVnM1WnVmcHNNbGpxNFVpMC8xbHZo
K3dqQ2hQNGtxS09KMnF4cQo0Umdxc2FoRFlWdlRIOXc3alhieUxlaU5kZDhYTTJ3OVUvdDd5MEZmLzl5aTBHRTQ0WmE0ckYyTE45ZDExVFBB
Cm1SR3VuVUhCY25XRXZnSkJRbDluSkVpVTBac252Z2MvdWJoUGdYUlI0WHEzN1owajRyN2cxU2dFRXp3eEE1N2QKZW15UHhnY1l4bi9lUjQ0
L0tKNEVCcytsVkRSM3ZleUptK2tYUTk5YjIxLytqaDVYb3MxQW5YNWlJdHJlR0NjPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCi0tLS0t
QkVHSU4gQ0VSVElGSUNBVEUtLS0tLQpNSUlDR3pDQ0FhR2dBd0lCQWdJUVFkS2QwWExxN3FlQXdTeHM2UytIVWpBS0JnZ3Foa2pPUFFRREF6
QlBNUXN3CkNRWURWUVFHRXdKVlV6RXBNQ2NHQTFVRUNoTWdTVzUwWlhKdVpYUWdVMlZqZFhKcGRIa2dVbVZ6WldGeVkyZ2cKUjNKdmRYQXhG
VEFUQmdOVkJBTVRERWxUVWtjZ1VtOXZkQ0JZTWpBZUZ3MHlNREE1TURRd01EQXdNREJhRncwMApNREE1TVRjeE5qQXdNREJhTUU4eEN6QUpC
Z05WQkFZVEFsVlRNU2t3SndZRFZRUUtFeUJKYm5SbGNtNWxkQ0JUClpXTjFjbWwwZVNCU1pYTmxZWEpqYUNCSGNtOTFjREVWTUJNR0ExVUVB
eE1NU1ZOU1J5QlNiMjkwSUZneU1IWXcKRUFZSEtvWkl6ajBDQVFZRks0RUVBQ0lEWWdBRXpadlZuNENEQ3V3SlN2TVdTajVjejNlczNtY0ZE
UjBIdHR3VworMXFMRk52aWNXREV1a1dWRVltTzZnYmY5eW9XSEtTNXhjVXk0QVBnSG9JWU9JdlhSZGdLYW03bUFIZjdBbEY5Ckl0Z0ticHBi
ZDkvdytrSHNPZHgxeW1nSERCL3FvMEl3UURBT0JnTlZIUThCQWY4RUJBTUNBUVl3RHdZRFZSMFQKQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRF
RmdRVWZFS1dydDVMU0R2Nmt2aWVqTTl0aTZseU41VXdDZ1lJS29aSQp6ajBFQXdNRGFBQXdaUUl3ZTNsT1JsQ0V3a1NIUmh0RmNQOVltZDcw
L2FUU1ZhWWdMWFRXTkx4Qm8xQmZBU2RXCnRMNG5kUWF2RWk1MW1JMzhBakVBaS9WM2JOVElaYXJnQ3l6dUZKMG5ONlQ1VTZWUjVDbUQxL2lR
TVZ0Q253cjEKL3E0QWFPZU1TUSsyYjF0YkZmTG4KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQ=="
```

**Important Notes:**
- The `ca_cert_b64` value above is the base64-encoded content of your `~/.postgresql/root.crt` file
- The `dsn` includes your connection details from `cockroach.txt`
- Keep your existing Supabase secrets for now (we'll migrate gradually)

## 🏗️ Step 3: Create CockroachDB Table

Run the setup script to create the table and test the connection:

```bash
poetry run python scripts/setup_cockroachdb.py
```

This script will:
1. ✅ Test the CockroachDB connection
2. 🏗️ Create the `atbats_optimized` table with proper indexes
3. 🧪 Test basic operations (insert, select, JSONB queries)

## 🔄 Step 4: Update Your Application

### Option A: Gradual Migration (Recommended)

Keep both connections and gradually switch over:

```python
# In your app.py
import streamlit as st

# Try CockroachDB first, fallback to Supabase
try:
    from cockroach_streamlit_connection import get_cockroach_connection
    conn = get_cockroach_connection()
    st.success("🪳 Connected to CockroachDB")
except Exception as e:
    st.warning(f"⚠️ CockroachDB connection failed: {e}")
    # Fallback to existing Supabase connection
    from st_supabase_connection import SupabaseConnection
    conn = st.connection("supabase", type=SupabaseConnection)
    st.info("📊 Using Supabase (fallback)")
```

### Option B: Complete Switch

Replace the Supabase connection entirely:

```python
# Replace this line:
# from st_supabase_connection import SupabaseConnection
# conn = st.connection("supabase", type=SupabaseConnection)

# With this:
from cockroach_streamlit_connection import get_cockroach_connection
conn = get_cockroach_connection()
```

## 📊 Step 5: Test the Migration

1. **Start your Streamlit app:**
   ```bash
   poetry run streamlit run pitch_prospector/app.py
   ```

2. **Verify the connection:**
   - Check that you see "🪳 Connected to CockroachDB" message
   - Test a simple search query
   - Verify that results are returned

3. **Check the database:**
   - The table should be empty initially (unless you migrate data)
   - You can run queries directly in CockroachDB console

## 🚀 Step 6: Data Migration (Optional)

If you want to migrate your existing data from Supabase:

1. **Export from Supabase:**
   ```bash
   # Use your existing scripts to export data
   poetry run python scripts/check_date_range.py
   ```

2. **Import to CockroachDB:**
   ```bash
   # Create a migration script (see examples below)
   poetry run python scripts/migrate_data.py
   ```

## 🔍 Troubleshooting

### Common Issues

1. **Certificate errors:**
   - Ensure the base64 certificate in secrets.toml is correct
   - Check that the certificate file path is accessible

2. **Connection timeouts:**
   - CockroachDB may have different timeout settings
   - Adjust connection pool parameters if needed

3. **SSL mode issues:**
   - Ensure `sslmode=verify-full` is in your DSN
   - Verify the CA certificate is properly formatted

### Debug Commands

```bash
# Test connection manually
poetry run python -c "
from cockroach_streamlit_connection import get_cockroach_connection
conn = get_cockroach_connection()
print('✅ Connection successful')
"

# Check table schema
poetry run python scripts/setup_cockroachdb.py
```

## 📈 Performance Considerations

### CockroachDB Advantages

- **Unlimited storage** - No more 0.4GB limits
- **Better JSONB performance** - Optimized for your pitch sequence data
- **Global distribution** - Lower latency for users worldwide
- **Horizontal scaling** - Can handle millions of records efficiently

### Migration Benefits

- **Immediate storage relief** - No more storage constraints
- **Better query performance** - Optimized indexes and distribution
- **Future scalability** - Ready for massive data growth
- **Cost optimization** - Better pricing for large datasets

## 🎯 Next Steps

After successful migration:

1. **Monitor performance** - Compare query times with Supabase
2. **Scale data ingestion** - Increase batch sizes and frequency
3. **Optimize queries** - Leverage CockroachDB-specific features
4. **Plan for growth** - Consider data partitioning strategies

## 📚 Additional Resources

- [CockroachDB Documentation](https://www.cockroachlabs.com/docs/)
- [PostgreSQL Compatibility Guide](https://www.cockroachlabs.com/docs/stable/postgresql-compatibility.html)
- [JSONB Performance Tips](https://www.cockroachlabs.com/docs/stable/jsonb.html)

---

**🎉 Congratulations!** You're now ready to scale your Pitch Prospector application to handle unlimited baseball data with CockroachDB. 