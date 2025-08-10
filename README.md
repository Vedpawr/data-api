# Moveworks Data Pipeline

A robust Python-based data pipeline that extracts data from the Moveworks Analytics API and loads it into Snowflake with comprehensive analytics views.

## 🎯 Overview

This pipeline provides:
- **Initial Historical Load**: Bulk load historical data with user-defined date ranges
- **Daily Incremental Sync**: Automated daily updates using UPSERT operations
- **Analytics Views**: Pre-built views for BI tools and reporting
- **Error Handling**: Comprehensive retry logic and error recovery
- **Monitoring**: Detailed logging and progress tracking

## 📋 Prerequisites

### System Requirements
- Python 3.8 or higher
- Network access to Moveworks API and Snowflake

### Python Dependencies
```bash
pip3 install -r requirements.txt
```

### Running a python virtual env 
Installing the required dependencies in your system can throw error or challenges. We strongly recommend you to run a virtual environment of python and then install the dependencies
to enable a virtual env run the following command. 
``` bash
python3 -m venv moveworks_env
source moveworks_env/bin/activate
```
install the dependencies again after this command. 


### Required Credentials
1. **Moveworks API Access Token**
   - Contact your Moveworks admin for API credentials

2. **Snowflake Account Details**
   - Account identifier (e.g., `xyz12345.us-east-1`)
   - Username and password
   - Warehouse, database, and schema names
   - Optional: Role (if required)

## 🚀 Quick Start

### 1. Initial Setup
```bash
python3 moveworks_pipeline.py setup
```
This interactive setup will prompt for:
- Moveworks API token
- Snowflake connection details
- Pipeline configuration (schedule, analytics views, etc.)

### 2. Historical Data Load
```bash
python3 moveworks_pipeline.py initial-load
```
- Choose your historical date range (recommended: 30-90 days to start)
- Loads data for: conversations, interactions, plugin-calls, plugin-resources, users
- Creates analytics views if configured

### 3. Start Daily Automation
```bash
python3 moveworks_pipeline.py start
```
- Runs daily at configured time (default: 22:00 PST)
- Uses UPSERT operations to prevent duplicates
- Updates analytics views automatically

## 📊 Data Schema

### Tables Created
| Table Name | Description | Primary Key |
|------------|-------------|-------------|
| `MOVEWORKS_CONVERSATIONS` | Conversation metadata and routing info | ID |
| `MOVEWORKS_INTERACTIONS` | Individual user and bot messages | ID |
| `MOVEWORKS_PLUGIN_CALLS` | Plugin execution logs | ID |
| `MOVEWORKS_PLUGIN_RESOURCES` | Resources used by plugins (KB articles, etc.) | ID |
| `MOVEWORKS_USERS` | User profile and activity data | ID |

### Analytics Views
| View Name | Purpose |
|-----------|---------|
| `V_TOTAL_CONVERSATIONS` | Daily conversation counts by route and domain |
| `V_TOTAL_INTERACTIONS` | Daily interaction metrics by platform and type |
| `V_ACTIVE_USERS` | Daily active user counts by platform |
| `V_KNOWLEDGE_PERFORMANCE` | Knowledge base article performance metrics |
| `V_DAILY_SUMMARY` | High-level daily KPIs and success rates |

## 🔧 Command Reference

### Setup and Configuration
```bash
# Initial configuration
python3 moveworks_pipeline.py setup

# Check current configuration
python3 moveworks_pipeline.py status

# Reset initial load flag (allows re-running initial load)
python3 moveworks_pipeline.py reset
```

### Data Loading
```bash
# Historical data load (interactive date selection)
python3 moveworks_pipeline.py initial-load

# Run daily sync once (manual)
python3 moveworks_pipeline.py run

# Start scheduled daily sync (runs continuously)
python3 moveworks_pipeline.py start
```

## ⚙️ Configuration Options

The pipeline stores configuration in `moveworks_config.json`. Key settings:

```json
{
  "pipeline": {
    "create_views": true,
    "daily_lookback_days": 1,
    "schedule_time": "22:00",
    "timezone": "US/Pacific",
    "use_upsert": true
  }
}
```

### Configuration Parameters
- **create_views**: Whether to create/update analytics views
- **daily_lookback_days**: How many days back to sync (default: 1)
- **schedule_time**: Daily sync time in HH:MM format (24-hour)
- **use_upsert**: Use MERGE operations to prevent duplicates (recommended: true)

## 📁 File Structure

```
moveworks_pipeline/
├── moveworks_pipeline.py      # Main pipeline script
├── moveworks_config.json      # Configuration file (created by setup)
├── logs/                      # Log files directory
│   └── moveworks_pipeline_YYYYMMDD.log
└── README.md                  # This file
```

## 📈 Monitoring and Logs

### Log Files
- Location: `logs/moveworks_pipeline_YYYYMMDD.log`
- Includes: API calls, data counts, errors, performance metrics
- New log file created daily

### Key Metrics to Monitor
```bash
# Check recent log entries
tail -f logs/moveworks_pipeline_$(date +%Y%m%d).log

# Look for specific patterns
grep "ERROR" logs/moveworks_pipeline_*.log
grep "Successfully loaded" logs/moveworks_pipeline_*.log
grep "MERGE completed" logs/moveworks_pipeline_*.log
```

### Success Indicators
- ✅ "Successfully connected to Snowflake"
- ✅ "Retrieved X records from page Y"
- ✅ "Successfully loaded X rows to TABLE_NAME"
- ✅ "MERGE completed - Inserted: X, Updated: Y"
- ✅ "Created view: VIEW_NAME"

## 🔍 Data Validation

### Quick Data Quality Checks
```sql
-- Check row counts across all tables
SELECT 'CONVERSATIONS' as table_name, COUNT(*) as row_count FROM MOVEWORKS_CONVERSATIONS
UNION ALL
SELECT 'INTERACTIONS', COUNT(*) FROM MOVEWORKS_INTERACTIONS
UNION ALL
SELECT 'PLUGIN_CALLS', COUNT(*) FROM MOVEWORKS_PLUGIN_CALLS
UNION ALL
SELECT 'PLUGIN_RESOURCES', COUNT(*) FROM MOVEWORKS_PLUGIN_RESOURCES
UNION ALL
SELECT 'USERS', COUNT(*) FROM MOVEWORKS_USERS;

-- Check data freshness
SELECT 
    MAX(CREATED_AT) as latest_conversation,
    MAX(LOAD_TIMESTAMP) as latest_load
FROM MOVEWORKS_CONVERSATIONS;

-- Verify UPSERT operations (check for duplicates)
SELECT ID, COUNT(*) as duplicate_count 
FROM MOVEWORKS_CONVERSATIONS 
GROUP BY ID 
HAVING COUNT(*) > 1;
```

### Analytics View Testing
```sql
-- Test each analytics view
SELECT * FROM V_DAILY_SUMMARY ORDER BY activity_date DESC LIMIT 7;
SELECT * FROM V_TOTAL_CONVERSATIONS WHERE conversation_date >= CURRENT_DATE - 7;
SELECT * FROM V_ACTIVE_USERS WHERE activity_date >= CURRENT_DATE - 7;
```

## 🚨 Troubleshooting

### Common Issues and Solutions

#### 1. API Authentication Errors
```
Error: 401 Unauthorized
```
**Solution**: Verify API token in configuration
```bash
python3 moveworks_pipeline.py setup  # Re-enter credentials
```

#### 2. Snowflake Connection Issues
```
Error: Failed to connect to Snowflake
```
**Solutions**:
- Check network connectivity
- Verify account identifier format
- Confirm user permissions
- Test connection manually:
```sql
-- Test in Snowflake console
SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_WAREHOUSE();
```

#### 3. Rate Limiting
```
Warning: Rate limited on /endpoint. Waiting for X seconds.
```
**Solution**: This is normal - the script will automatically retry. Consider:
- Running during off-peak hours
- Reducing date range for initial loads

#### 4. Data Type Errors
```
Error: Expression type does not match column data type
```
**Solution**: Usually resolved in the fixed script version. If persistent:
- Check for API schema changes
- Verify pandas version compatibility

#### 5. Table Creation Failures
```
Error: Failed to create table MOVEWORKS_X
```
**Solutions**:
- Verify Snowflake permissions (CREATE TABLE)
- Check database and schema exist
- Ensure role has necessary privileges

### Emergency Recovery

#### Reset Everything
```bash
# Reset pipeline state
python3 moveworks_pipeline.py reset

# Manual Snowflake cleanup (if needed)
DROP TABLE IF EXISTS MOVEWORKS_CONVERSATIONS;
DROP TABLE IF EXISTS MOVEWORKS_INTERACTIONS;
DROP TABLE IF EXISTS MOVEWORKS_PLUGIN_CALLS;
DROP TABLE IF EXISTS MOVEWORKS_PLUGIN_RESOURCES;
DROP TABLE IF EXISTS MOVEWORKS_USERS;

# Drop views
DROP VIEW IF EXISTS V_TOTAL_CONVERSATIONS;
DROP VIEW IF EXISTS V_TOTAL_INTERACTIONS;
DROP VIEW IF EXISTS V_ACTIVE_USERS;
DROP VIEW IF EXISTS V_KNOWLEDGE_PERFORMANCE;
DROP VIEW IF EXISTS V_DAILY_SUMMARY;
```

## 🔐 Security Best Practices

1. **Credential Management**
   - Store API tokens securely
   - Use Snowflake service accounts with minimal permissions
   - Rotate credentials regularly

2. **Network Security**
   - Run in secure network environment
   - Use Snowflake IP whitelisting if required
   - Consider VPN for API access

3. **Data Access**
   - Grant minimal Snowflake permissions
   - Restrict access to analytics views as needed
   - Monitor data access patterns

## 📧 Support and Maintenance

### Regular Maintenance Tasks
- **Weekly**: Review log files for errors
- **Monthly**: Verify data quality and completeness
- **Quarterly**: Review and optimize analytics views
- **Annually**: Rotate API credentials

### Getting Help
1. Check log files first: `logs/moveworks_pipeline_*.log`
2. Review this README for common issues
3. Test individual components:
   ```bash
   # Test configuration
   python3 moveworks_pipeline.py status
   
   # Test single day sync
   python3 moveworks_pipeline.py run
   ```

### Performance Optimization
- **Large Datasets**: Consider chunking initial loads by smaller date ranges
- **View Performance**: Add indexes on frequently queried columns
- **Storage Costs**: Implement data retention policies in Snowflake

## 📊 Sample Analytics Queries

### Daily KPI Dashboard
```sql
-- Last 30 days summary
SELECT 
    activity_date,
    total_conversations,
    active_users,
    knowledge_success_rate
FROM V_DAILY_SUMMARY 
WHERE activity_date >= CURRENT_DATE - 30
ORDER BY activity_date DESC;
```

### Top Knowledge Articles
```sql
-- Most cited knowledge articles this month
SELECT 
    knowledge_name,
    SUM(times_cited) as total_citations,
    AVG(helpful_feedback) as avg_helpful_feedback
FROM V_KNOWLEDGE_PERFORMANCE 
WHERE report_date >= DATE_TRUNC('month', CURRENT_DATE)
GROUP BY knowledge_name
ORDER BY total_citations DESC
LIMIT 20;
```

### Platform Usage Trends
```sql
-- Platform adoption over time
SELECT 
    activity_date,
    platform,
    active_users,
    LAG(active_users) OVER (PARTITION BY platform ORDER BY activity_date) as prev_day_users
FROM V_ACTIVE_USERS 
WHERE activity_date >= CURRENT_DATE - 30
ORDER BY activity_date DESC, platform;
```

---

## 📝 Version History

- **v1.0**: Initial release with basic ETL functionality
- **v1.1**: Added UPSERT support and improved error handling
- **v1.2**: Enhanced analytics views and monitoring capabilities

For questions or issues, please review the troubleshooting section or check the log files for detailed error information.
