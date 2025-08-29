# 🔄 Auto-Refresh System for Pitch Prospector

## Overview
The Pitch Prospector app now includes a **background threading system** that automatically keeps your MLB data up-to-date without any user visibility or intervention.

## Background Threading Philosophy
**"Set it and forget it"** - This system operates using background threads:
- ✅ **Runs invisibly** in the background
- ✅ **Non-blocking startup** - app loads immediately, refresh runs in background
- ✅ **No user notifications** about refresh operations
- ✅ **No status indicators** showing when refreshes happen
- ✅ **No timing information** displayed to users
- ✅ **Silent operation** - users only see fresh data, never the refresh process

## How It Works

### 1. **Daily Auto-Refresh (Streamlit Caching)**
- **Frequency**: Runs once per day using Streamlit's `@st.cache_data(ttl=86400)`
- **Trigger**: Automatically runs 5 seconds after app startup (non-blocking)
- **Logic**: Only refreshes if data is more than 1 day old
- **Efficiency**: Uses caching to prevent unnecessary API calls
- **Visibility**: **Completely invisible to users**

### 2. **Background Worker (Continuous Updates)**
- **Frequency**: Checks every 6 hours while the app is running
- **Trigger**: Runs in a background thread that starts with the app (non-blocking)
- **Logic**: Only refreshes if data is more than 6 hours old
- **Smart**: Skips refresh if data is already fresh
- **Visibility**: **Completely invisible to users**

### 3. **Manual Refresh Button**
- **Purpose**: Allows users to force a refresh when needed
- **Tracking**: Shows when the last manual refresh occurred
- **Integration**: Works alongside the auto-refresh system

## Features

### **Smart Refresh Logic**
- ✅ Only refreshes when data is stale
- ✅ Prevents duplicate data insertion
- ✅ Handles API rate limits gracefully
- ✅ Error recovery with exponential backoff

### **User Experience**
- 🎯 **Completely invisible** - users never see auto-refresh happening
- 📊 Subtle data freshness indicator (no mention of auto-refresh)
- 🔄 Manual refresh option always available for immediate updates

### **Performance Optimizations**
- 🚀 **Non-blocking startup** - app loads immediately, refresh runs in background
- 🚀 Batch processing for large datasets
- 💾 Efficient database operations with upsert
- 🧠 Memory-conscious data handling
- ⚡ Non-blocking background operations

## Technical Implementation

### **Auto-Refresh Functions**
```python
def auto_refresh_data():
    # Main refresh logic with error handling
    
@st.cache_data(ttl=86400)
def run_daily_auto_refresh():
    # Daily refresh with caching
    
def background_refresh_worker():
    # Continuous background monitoring
```

### **Database Operations**
- Uses existing `process_statcast_data_for_refresh()` function
- Leverages `insert_atbats_with_duplicate_prevention()` for safe insertion
- Maintains data integrity with transaction handling

### **Error Handling**
- Comprehensive exception catching
- Detailed logging for debugging
- Graceful fallbacks for failed operations
- Automatic retry mechanisms

## Configuration

### **Environment Variables**
The system automatically detects your Streamlit Cloud secrets or local environment variables:
- `COCKROACH_HOST`
- `COCKROACH_PORT` 
- `COCKROACH_DATABASE`
- `COCKROACH_USER`
- `COCKROACH_PASSWORD`

### **Customization Options**
- **Refresh Intervals**: Modify the `time.sleep()` values in `background_refresh_worker()`
- **Cache Duration**: Adjust the `ttl` parameter in `@st.cache_data()`
- **Stale Thresholds**: Change the hour thresholds for refresh decisions

## Monitoring & Debugging

### **Console Logs**
The system runs silently with minimal logging (only errors):
```
Background refresh system error: [any errors that occur]
```

### **User Interface Indicators**
- 🟢 **Green**: "Data current as of X hours ago" (< 24 hours old)
- 🔵 **Blue**: "Data from X hours ago" (24+ hours old)
- **No mention of auto-refresh or timing information**

## Benefits

1. **Always Fresh Data**: Users get the latest MLB stats automatically
2. **Reduced Manual Work**: No need to manually refresh data
3. **Better Performance**: Efficient, intelligent refresh scheduling
4. **Background Threading**: Runs invisibly using Python background threads
5. **User Focus**: Users focus on using the app, not managing data updates
6. **Reliability**: Robust error handling and recovery

## Future Enhancements

- **Webhook Integration**: Trigger refreshes based on external events
- **Analytics Dashboard**: Monitor refresh performance and success rates
- **Custom Schedules**: Allow users to set preferred refresh times
- **Notification System**: Alert users when data is updated

---

*The background threading system ensures your Pitch Prospector app always has the latest MLB data, providing users with the most current and accurate pitch sequence analysis.*
