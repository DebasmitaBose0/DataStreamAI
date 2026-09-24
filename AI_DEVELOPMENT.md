# AI-Assisted Engineering Workflow & Reflection
**Project:** DataStream AI — “From Raw Data to Intelligent Insights”

This document records the actual iterative software engineering cycle developed with AI pairing, documenting real debugging scenarios, discovered edge cases, manual interventions, and architectural refinements.

---

## Case Study: Resilient Event Cleaning & Pipeline Edge Cases

### 1. Problem Statement
In building an end-to-end OTT streaming event pipeline, streaming inputs vary in fidelity:
- Full batch feeds from CSV include `session_id`, `device`, `ip_address`, and `watch_time_mins`.
- Lightweight streaming packets (e.g. from unit test payloads, mobile push pings, or third-party webhooks) may omit peripheral dimensions such as `device` or `session_id`.
- The data cleaning engine (`pipeline/cleaning.py`) needs to normalize incoming event DataFrames without crashing if non-critical columns are missing, while rigorously filtering invalid event verbs (`hacked_action`), negative streaming durations, and duplicate IDs.

---

### 2. Initial AI-Generated Solution
The initial implementation assumed all standardized columns were always present in the incoming DataFrame:

```python
# Initial implementation assumption:
df_clean["device"] = df_clean["device"].fillna("Smart TV").astype(str).str.strip()
df_clean["session_id"] = df_clean["session_id"].fillna("sess_gen").astype(str).str.strip()
```

Additionally, in `kafka/consumer.py`, a malformed type hint was initially generated:
```python
def validate_event(self, event: Dict[str, Any]) -> Tuple_Bool_Reason := tuple:
```

---

### 3. Bugs & Issues Discovered
During test execution via `pytest` and module compilation testing:
1. **Python Syntax Error**:
   ```text
   SyntaxError: expected ':'
   File "kafka/consumer.py", line 38
     def validate_event(self, event: Dict[str, Any]) -> Tuple_Bool_Reason := tuple:
   ```
2. **Pandas Index/KeyError During Unit Testing**:
   When `TestCleaningAndTransformation.test_event_cleaning_deduplication` ran with a minimal event DataFrame:
   ```text
   KeyError: 'device'
   File "pipeline/cleaning.py", line 116, in clean_events
     df_clean["device"] = df_clean["device"].fillna("Smart TV").astype(str).str.strip()
   ```

---

### 4. Root Cause Debugging
1. **Syntax Issue**: An experimental Walrus/type alias pattern was accidentally written in place of a standard PEP 484 return type annotation `tuple`.
2. **Schema Rigidity**: The data cleaner assumed rigid presence of every non-key column before checking column headers, causing Pandas indexing to raise a `KeyError` when handling partial streaming schemas.

---

### 5. Manual & Collaborative Corrections
1. **Normalized Type Hinting**:
   ```python
   def validate_event(self, event: Dict[str, Any]) -> tuple:
       """Validates streaming event payload."""
   ```
2. **Adaptive Column Initialization**:
   Updated `pipeline/cleaning.py` to check for column existence and provide sensible default dimension values:
   ```python
   # Ensure optional columns are present
   if "device" not in df_clean.columns:
       df_clean["device"] = "Smart TV"
   else:
       df_clean["device"] = df_clean["device"].fillna("Smart TV").astype(str).str.strip()

   if "session_id" not in df_clean.columns:
       df_clean["session_id"] = "sess_gen"
   else:
       df_clean["session_id"] = df_clean["session_id"].fillna("sess_gen").astype(str).str.strip()
   ```

---

### 6. Verification & Final Results
After applying the refactored logic, the entire automated test suite was executed:
```bash
python -m pytest tests/ -v
```
**Outcome:**
- `11 passed in 5.56s` (100% test pass rate)
- Verified coverage across:
  - Email, Phone, and IPv4 PII redaction
  - Duplicate record detection
  - Invalid event type rejection
  - Out-of-bounds numeric rejection
  - Schema drift detection (new columns & missing columns)
  - Event deduplication and cleaning
  - Relational database query execution

---

## Developer Takeaways
1. **Design for Schema Evolution**: In real-time data engineering, client events will inevitably arrive with varying payloads. Pipelines must differentiate between **critical identity keys** (`user_id`, `event_type`, `timestamp`) and **enrichment attributes** (`device`, `session_id`).
2. **Layered Fallbacks Save Prototypes**: Implementing automatic zero-configuration SQLite and local JSON fallbacks ensured that testing never stalled due to external daemon availability.
