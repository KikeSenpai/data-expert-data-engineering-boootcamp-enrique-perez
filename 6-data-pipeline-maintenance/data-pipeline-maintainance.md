# Data Pipeline Maintenance

## Team Information

**Pacific Infra Group 2:** Divya Dhar, Aikya Shah

## Business Info

Like Salesforce - provides a SaaS platform to other companies. Companies can sign up, provide licenses to a certain number of their staff, as well as buy additional features/support.

## Pipeline Overview

### 5 Pipelines

1. **Profit** = subscription costs paid - expenses on all accounts
   - Unit level profit: profit / # of subscribers (+ info about cost per account)

2. **Growth:**
   - Increase in number of accounts per month
   - Increase in size of accounts that are about to renew (increase in $ increased from upgrade in account)

3. **Engagement**
   - How many users using technology in company
   - How many hours per day are all users on account spending time on account

4. **Aggregate pipeline to Executives/CFO** (ultimately presented to investors)
   - **Frequency:** Weekly

5. **Aggregate pipeline to Experiment team**
   - Data science team uses unit level/daily level data to conduct experiments on AB testing features being rolled out to different accounts
   - **Frequency:** Weekly preferred, monthly - to drive direction of product team

**Critical Pipelines:** The pipelines that will affect investors are the Profit, Growth, Engagement and Aggregate Pipeline to Investors - so the next few pages will be runbooks for each pipeline.

## Runbooks

### 1. Pipeline Name: Profit

**Types of data:**

- Revenue from accounts
- What is spent on assets, other services according to Ops team
- Aggregated salaries by team

**Owners:** Finance Team/Risk Team

- **Secondary Owner:** Data Engineering

**Common Issues:**

- Numbers don't align with numbers on accounts/filings - these numbers need to be verified by an accountant if so

**SLAs:**

- Numbers will be reviewed once a month by account team

**On-call schedule:**

- Monitored by BI in profit team, and folks rotate watching pipeline on weekly basis
- If something breaks, it needs to be fixed

### 2. Pipeline Name: Growth

**Types of data:** Changes made to the account type

- Number of users with license increased
- Account stopped subscribing
- Account continued subscription for the next calendar year

**Owners:** Accounts Team

- **Secondary Owner:** Data Engineer Team

**Common Issues:**

- Time series dataset - so the current status of an account is missing since AE team forgot to update
  - A clue that it's missing is a previous step that is required is missing (ex: only changes A, C, when step B is required to change to C)

**SLAs:**

- Data will contain latest account statuses by end of week

**On-call schedule:**

- No on call if pipeline fails, but pipeline will be debugged by team during working hours

### 3. Pipeline Name: Engagement

**Owners:** Software front-end Team

- **Secondary Owner:** Data Engineer Team

**Data Source:** Engagement metrics come from clicks from all users using platforms in different teams

**Common Issues:**

- Sometimes data associated with click will arrive to Kafka queue extremely late - much after the data has already been aggregated for a downstream pipeline
- If Kafka goes down, all user clicks from website will not be sent to Kafka, therefore not sent to the downstream metrics
- Sometimes the same event will come through the pipeline multiple times - data must be de-duplicated

**SLAs:**

- Data will arrive within 48hrs - if latest timestamp > the current timestamp - 48 hrs, then the SLA is not met.
- Issues will be fixed within 1 week

**On-call schedule:**

- One person on DE team owns pipeline each week - there is a contact on SWE team for questions
- Next week - 30 min meeting to transfer on-boarding to the next person

### 4. Pipeline Name: Aggregated data for executives and investors

**Owners:** Business Analytics team

- **Secondary Owner:** Data Engineer team

**Common Issues:**

- Spark joins to join accounts to revenue, and engagement may fail - a lot of data is involved in the joins and there may be OOM issues
- Issues with stale data with previous pipelines - queue backfills periodically
- Missing data may cause issues with NA or divide by 0 errors

**SLAs:**

- Issues will be fixed by end of month, when reports are given to executives and investors

**On-call schedule:**

- Around last week of month, data engineers are monitoring that pipelines of the data from the month are running smoothly
