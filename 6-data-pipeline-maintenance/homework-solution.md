# Data Pipeline Maintenance - Homework Solution

## Team Structure

**Data Engineering Team:** 4 Data Engineers

- Engineer A (Senior)
- Engineer B (Mid-level)
- Engineer C (Mid-level)
- Engineer D (Junior)

## 1. Pipeline Ownership Assignment

### Profit Pipelines

**Unit-level Profit Pipeline (for experiments)**

- **Primary Owner:** Engineer A (Senior)
- **Secondary Owner:** Engineer B
- **Rationale:** Unit-level calculations require deep understanding of business metrics and experimentation frameworks

**Aggregate Profit Pipeline (for investors)**

- **Primary Owner:** Engineer B
- **Secondary Owner:** Engineer A
- **Rationale:** Investor-facing metrics require high reliability and accuracy; senior oversight essential

### Growth Pipelines

**Aggregate Growth Pipeline (for investors)**

- **Primary Owner:** Engineer C
- **Secondary Owner:** Engineer D
- **Rationale:** Growth metrics are critical for investor reporting; mid-level engineer with junior backup

**Daily Growth Pipeline (for experiments)**

- **Primary Owner:** Engineer D
- **Secondary Owner:** Engineer C
- **Rationale:** Good learning opportunity for junior engineer with mid-level support

### Engagement Pipeline

**Aggregate Engagement Pipeline (for investors)**

- **Primary Owner:** Engineer A (Senior)
- **Secondary Owner:** Engineer C
- **Rationale:** Engagement data is complex and investor-critical; requires senior expertise

## 2. Fair On-Call Schedule

### Rotation Structure

**Weekly Rotation** with the following considerations:

#### Standard Schedule (Non-Holiday Weeks)

- **Week 1:** Engineer A (Primary on-call)
- **Week 2:** Engineer B (Primary on-call)
- **Week 3:** Engineer C (Primary on-call)
- **Week 4:** Engineer D (Primary on-call)

#### Holiday Considerations

- **Major Holidays (Christmas, New Year, Thanksgiving):**
  - Senior engineers (A & B) volunteer for holiday coverage
  - Double compensation/comp time provided
  - Maximum 2 consecutive holiday weeks per person per year

- **Personal Time Off:**
  - Minimum 2 weeks notice required for planned PTO during on-call week
  - Team member must arrange coverage swap
  - Emergency PTO: Secondary owner automatically assumes primary role

#### Escalation Path

1. **Primary On-call** (4-hour response SLA)
2. **Secondary On-call** (if primary unavailable - 2-hour response SLA)
3. **Senior Engineer** (if both unavailable - 1-hour response SLA)
4. **Engineering Manager** (critical issues only)

#### Fairness Measures

- **Load Balancing:** Track incident volume per person quarterly
- **Skill Development:** Junior engineers paired with seniors during complex incidents
- **Compensation:** On-call stipend + overtime for incidents requiring >2 hours
- **Recovery Time:** Day off after handling major incidents (>6 hours)

## 3. Runbooks for Investor-Facing Pipelines

### Pipeline 1: Aggregate Profit (Investor Reporting)

#### **Data Sources**

- Subscription revenue database
- Operational expense tracking system
- Payroll/HR systems
- Cloud infrastructure costs

#### **Potential Issues**

1. **Revenue Recognition Timing**
   - **Symptom:** Profit numbers don't match accounting reports
   - **Cause:** Subscription renewals processed in different systems at different times
   - **Impact:** Investor reports show incorrect profitability

2. **Expense Allocation Errors**
   - **Symptom:** Negative profit margins when should be positive
   - **Cause:** Double-counting expenses or missing cost centers
   - **Impact:** Misleading financial health indicators

3. **Currency Conversion Issues**
   - **Symptom:** Profit fluctuations unrelated to business performance
   - **Cause:** Exchange rate calculation errors for international customers
   - **Impact:** Inaccurate global profit reporting

4. **Data Freshness Problems**
   - **Symptom:** Week-old data in monthly investor reports
   - **Cause:** Upstream systems delayed or failed to export
   - **Impact:** Outdated financial position reported to investors

### Pipeline 2: Aggregate Growth (Investor Reporting)

#### **Data Sources**

- Customer acquisition system
- Account management platform
- Subscription upgrade/downgrade events
- Churn tracking database

#### **Potential Issues**

1. **Account State Inconsistency**
   - **Symptom:** Growth numbers showing impossible transitions
   - **Cause:** Account status updates missing intermediate steps
   - **Impact:** Inflated or deflated growth metrics

2. **Duplicate Customer Counting**
   - **Symptom:** Growth rates exceeding market possibilities
   - **Cause:** Same customer counted multiple times due to data integration issues
   - **Impact:** Overstated growth performance

3. **Churn Misclassification**
   - **Symptom:** Retention rates inconsistent with business reality
   - **Cause:** Temporary account suspensions counted as churn
   - **Impact:** Incorrect customer lifetime value calculations

4. **Time Zone Boundary Issues**
   - **Symptom:** Growth spikes/drops at month boundaries
   - **Cause:** Customer events crossing time zones incorrectly attributed
   - **Impact:** Misleading growth trend analysis

### Pipeline 3: Aggregate Engagement (Investor Reporting)

#### **Data Sources**

- User activity tracking (Kafka streams)
- Session management systems
- Feature usage analytics
- Mobile/web application logs

#### **Potential Issues**

1. **Event Deduplication Failures**
   - **Symptom:** Engagement metrics showing superhuman usage patterns
   - **Cause:** Same user events processed multiple times
   - **Impact:** Overstated product engagement and user value

2. **Kafka Queue Backlog**
   - **Symptom:** Engagement dropping to zero suddenly
   - **Cause:** Real-time event processing system overwhelmed or down
   - **Impact:** Missing critical engagement data for investor reports

3. **Bot Traffic Contamination**
   - **Symptom:** Engagement metrics inconsistent with user feedback
   - **Cause:** Automated traffic not filtered from user activity
   - **Impact:** Inflated engagement numbers misleading investors

4. **Session Timeout Misconfiguration**
   - **Symptom:** Average session times unrealistically long or short
   - **Cause:** Session boundary detection failing
   - **Impact:** Incorrect user engagement depth metrics

5. **Cross-Platform Tracking Gaps**
   - **Symptom:** Engagement drops during known high-usage periods
   - **Cause:** Mobile or web tracking failing to capture all user interactions
   - **Impact:** Underreported engagement affecting investor confidence

#### **Common Cross-Pipeline Issues**

1. **Spark Job Memory Issues (OOM Failures)**
   - **Symptom:** Jobs failing with OutOfMemoryError during large joins
   - **Cause:** Data skew in customer, revenue, and engagement joins
   - **Solution:** Enable Adaptive Query Execution (AQE) with `spark.sql.adaptive.enabled = true`
   - **Impact:** Pipeline failures during investor report generation periods

2. **Data Skew in Spark Pipelines**
   - **Symptom:** Some tasks taking significantly longer than others
   - **Cause:** Uneven data distribution across partitions
   - **Solution:** Implement salt-based redistribution or bucketing strategies
   - **Impact:** Extended processing times affecting SLA compliance

3. **Missing Data & Schema Changes**
   - **Symptom:** Pipeline failures or incorrect metrics after upstream changes
   - **Cause:** Upstream systems modify schema without notification
   - **Solution:** Implement schema validation and graceful handling of missing columns
   - **Impact:** Broken investor reports requiring manual intervention

4. **Backfill Cascade Management**
   - **Symptom:** Backfills not triggering downstream pipeline updates
   - **Cause:** Lack of coordination between pipeline dependencies
   - **Solution:** Implement table versioning strategy (table_prod → table_backfill → table_v2)
   - **Impact:** Inconsistent data across dependent systems

5. **Excessive Cloud Costs (FinOps Issues)**
   - **Primary Cost Drivers:**
     - **I/O Operations:** Reading/writing large datasets repeatedly
     - **Compute:** Inefficient query patterns and resource allocation
     - **Storage:** Duplicated data models and unnecessary retention

   **Common Causes:**
   - Duplicated data models across teams
   - Inefficient pipelines not using cumulative design patterns
   - Excessive backfills without sampling strategies
   - Poor data partitioning preventing predicate push-down optimization

6. **Data Quality Cascading Failures**
   - **Symptom:** Bad data in one pipeline affecting all downstream aggregations
   - **Cause:** Lack of data validation at pipeline boundaries
   - **Impact:** All investor metrics potentially compromised

7. **Infrastructure Dependencies**
   - **Symptom:** Multiple pipeline failures during maintenance windows
   - **Cause:** Database locks during backup windows affecting all pipelines
   - **Impact:** Delayed or missing investor reports

8. **Regulatory Compliance Issues**
   - **Symptom:** Incomplete datasets in investor reports
   - **Cause:** GDPR/data privacy requirements affecting data availability
   - **Impact:** Unable to provide complete investor metrics

9. **Inefficient Data Processing Patterns**
   - **Symptom:** High compute costs and slow processing times
   - **Cause:** Not leveraging sampling when possible for large datasets
   - **Solution:** Implement statistical sampling for non-critical analysis
   - **Impact:** Unnecessary resource consumption and delayed reports

10. **Poor Data Partitioning**
    - **Symptom:** Queries scanning entire datasets instead of relevant partitions
    - **Cause:** Incorrect partitioning strategy preventing predicate push-down
    - **Solution:** Re-partition data by date/business unit for optimal query performance
    - **Impact:** Increased query costs and processing times

## Emergency Contacts & Escalation

### Business Stakeholders

- **CFO Office:** For profit-related discrepancies
- **VP Sales:** For growth metric questions
- **Product Team:** For engagement data concerns

### Technical Dependencies

- **DevOps Team:** Infrastructure and deployment issues
- **Security Team:** Data access and compliance issues
- **Database Team:** Query performance and data availability

## SLA Commitments

### Investor-Facing Pipelines

- **Data Freshness:** Within 24 hours of source system updates
- **Incident Response:** Critical issues resolved within 4 hours
- **Monthly Reports:** Delivered 5 business days before investor meetings
- **Data Accuracy:** 99.9% accuracy verified through automated testing

