## Product: HelloFresh Mobile App

### User Journey

**Discovery & Onboarding (Month 0)**

* I first encountered HelloFresh through a targeted social media ad and installed the HelloFresh mobile app in July 2023.
* The signup flow was smooth: I completed a quick dietary survey, entered my address, and set my delivery preferences within minutes.

**Early Usage (Months 1–3)**

* My first box arrived seamlessly. I appreciated the step-by-step recipe walkthroughs and high-quality ingredient photos, which made cooking approachable even on busy weeknights.
* I loved how easy it was to browse weekly menus and mark meals as favorites for future orders.

**Engagement & Habit Formation (Months 4–12)**

* HelloFresh’s weekly push reminders to select recipes helped me build a routine of planning meals ahead of time.
* I regularly used the in-app search and filtering by cuisine, dietary preference, and prep time—making it effortless to discover new recipes.
* Many users find HelloFresh to be highly convenient and time-saving, with ingredients delivered directly to their homes and easy-to-follow recipes that reduce grocery shopping trips.

**Maturation & Current Experience (Year 2)**

* Today, I rely on the personalized recommendations and my digital cookbook of saved meals every week. I’m low-key obsessed with how the app displays meal listings—handy tags, cooking time, and food-porn photography make the experience delightful.
* A notable pain point: cancelling or pausing my subscription through the app felt clunky and required contacting support, which was frustrating.
* Overall, continuous UI refinements and new features (nutrition filters, smart suggestions) have kept me engaged two years in.

### Proposed Experiments

#### Experiment 1: Personalized Recipe Recommendations

* **Test Cells Allocation:**
  * Control (no personalization): 33% of active users
  * Variant A (collaborative filtering-based recs): 33%
  * Variant B (popularity-based recs): 34%

* **Conditions:**
  * Control sees default menu order
  * Variant A sees recipes ranked by similarity to past orders
  * Variant B sees top-rated meals across all users

* **Hypothesis & Metrics:**
  * *Leading:* Recipe click-through rate, time spent browsing
  * *Lagging:* Weekly meals ordered per user, 4-week retention

#### Experiment 2: Push Notification Timing

* **Test Cells Allocation:**
  * Control: Wednesday at 10 AM (current)
  * Variant A: Thursday at 6 PM
  * Variant B: Monday at 8 AM
  * Each group: \~33% of users

* **Conditions:**
  * Users receive the same reminder content at different times

* **Hypothesis & Metrics:**
  * *Leading:* Push open rate, time-to-selection
  * *Lagging:* Plan completion rate, subscription churn over 8 weeks

#### Experiment 3: Favorites Quick Access

* **Test Cells Allocation:** 50% Control vs. 50% Variant

* **Conditions:**
  * Control: Existing navigation
  * Variant: New "Favorites" tab on home screen for one-tap access

* **Hypothesis & Metrics:**
  * *Leading:* Favorites-tab click rate, average session duration
  * *Lagging:* Repeat order rate of favorited meals, Net Promoter Score
