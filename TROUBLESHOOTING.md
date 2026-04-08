# Dashboard Troubleshooting Guide

## Common Errors and Solutions

### Error 1: "Chart is not defined"
**Cause:** Chart.js library not loaded or loaded too late

**Fixed in:** `dashboard/templates/index.html`
- Moved Chart.js `<script>` tag from `<head>` to bottom of `<body>`
- Updated to Chart.js v4.4.0 (stable version)
- Now loads BEFORE custom JavaScript code

**Verify fix:**
1. Open browser DevTools (F12)
2. Go to Console tab
3. Type: `typeof Chart`
4. Should return: `"function"` (not "undefined")

---

### Error 2: "Cannot read properties of undefined (reading 'forEach')"
**Cause:** API `/api/data` didn't return `columns` field

**Fixed in:**
1. `dashboard/app.py` - Added `"columns": df.columns.tolist()` to response
2. `dashboard/templates/index.html` - Added null checks in `loadTable()`

**Verify fix:**
1. Open: http://127.0.0.1:5000/api/data?page=1&per_page=5
2. JSON should contain: `"columns": ["sale_id", "date_key", ...]`

---

### Error 3: Dashboard shows old code after updates
**Cause:** Python process cached old code, browser cached old JavaScript

**Solutions:**

**A) Restart Dashboard:**
```bash
# Press Ctrl+C in dashboard terminal, then:
python main.py --dashboard
```

**B) Hard Refresh Browser:**
- Windows/Linux: `Ctrl + Shift + R` or `Ctrl + F5`
- Mac: `Cmd + Shift + R`

**C) Use Restart Script:**
```powershell
powershell .\restart_dashboard.ps1
```

---

## How to Verify Dashboard is Working

### 1. Test APIs Manually
```powershell
# Test stats
Invoke-RestMethod -Uri "http://127.0.0.1:5000/api/stats"

# Test data (should have 'columns' field)
Invoke-RestMethod -Uri "http://127.0.0.1:5000/api/data?page=1&per_page=5"

# Test run ETL
Invoke-RestMethod -Uri "http://127.0.0.1:5000/api/run_etl" -Method Post
```

### 2. Check Browser Console
1. Open dashboard: http://127.0.0.1:5000
2. Press F12 (Developer Tools)
3. Go to Console tab
4. Should see NO red errors
5. Check Network tab - all requests should be green (200 status)

### 3. Test Run ETL Button
1. Click "Run ETL" button
2. Should see:
   - Button changes to "⏳ Running..."
   - Popup appears with:
     - ✅ ETL Complete!
     - 📊 Rows Loaded: X
     - ⏱️ Time: X.XXs
     - ✅ Validation status
   - Stats cards update automatically
   - Chart refreshes
   - Table refreshes

---

## Files Modified

### dashboard/app.py
- Line 116: Added `"columns": df.columns.tolist()`

### dashboard/templates/index.html
- Line 8: Removed Chart.js from <head>
- Line 118: Added Chart.js before custom scripts
- Line 145-175: Enhanced `loadTable()` with null checks
- Line 122-129: Fixed `loadStats()` to use correct field names
- Line 131-155: Fixed `loadChart()` to handle object format
- Line 177-233: Enhanced `runETL()` with detailed popup

---

## Quick Test Commands

```powershell
# 1. Stop all Python processes
Get-Process python | Stop-Process -Force

# 2. Start fresh dashboard
python main.py --dashboard

# 3. Test in browser
# Open: http://127.0.0.1:5000
# Hard refresh: Ctrl+Shift+R
# Click Run ETL button
```

---

## Expected Behavior (After Fixes)

### On Page Load:
- ✅ No console errors
- ✅ Stats cards show numbers
- ✅ Chart displays bars
- ✅ Table shows data rows

### When Clicking "Run ETL":
- ✅ Button disabled during execution
- ✅ Button text changes to "⏳ Running..."
- ✅ Popup shows detailed results
- ✅ All sections auto-refresh
- ✅ Button re-enabled after completion

### Browser Console:
- ✅ `Chart` is defined (test with `typeof Chart`)
- ✅ No "ReferenceError"
- ✅ No "TypeError"
- ✅ All fetch requests succeed (200 status)
