# On-Call Runbook for EcZachly Inc Growth Pipeline

## Team Ownership

**Primary Owner:** Zach
**Secondary Owner:** Lulu

## Common Issues

### Upstream Datasets

#### Web Site Events

**Common anomalies:**

- Sometimes referrer is NULL too much, this is fixed downstream but we are alerted about because it messes with the metrics

#### User Database Exports

**Common issues:**

- Export might fail to be extracted on a given day, when this happens, just use yesterday's export for today

### Downstream Consumers

- Experimentation platform
- Dashboards

## SLAs

**Data Landing Time:** The data should land 4 hours after UTC midnight
