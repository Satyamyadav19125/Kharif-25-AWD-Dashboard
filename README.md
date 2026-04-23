# Kharif 25 AWD — Field Dashboard

A professional split-layout field dashboard for the Kharif 25 AWD (Alternate Wetting & Drying) study in Punjab.

## What's inside

```
kharif_dashboard/
├── app.py                  ← Flask app (API endpoints)
├── requirements.txt        ← Python dependencies
├── render.yaml             ← Render deploy config
├── templates/
│   └── index.html          ← Dashboard UI
└── static/
    ├── farm_records.json   ← 380 matched farm records
    └── village_stats.json  ← Village-level aggregates
```

## Features

- **Split layout**: Analytics panel left, full map right
- **Map**: Dark basemap, tubewell markers, farm polygons
- **Markers**: Color = Group (A/B/C), border = compliance status
- **Analytics tabs**: Overview charts, village table, searchable farm list
- **Charts**: Group donut, compliance donut, acres bar chart, village progress bars
- **Farm detail popup**: Slides in on map click with full farmer info
- **Search**: Jump to any farmer/village from the map overlay

## Deploy on Render (free)

### Step 1 — Push to GitHub
1. Create a GitHub account at github.com (free)
2. Create a new repository (e.g. `kharif-dashboard`)
3. Upload ALL files from this folder maintaining the folder structure
   - You can drag-and-drop the entire folder in the GitHub web interface

### Step 2 — Deploy on Render
1. Go to https://render.com and sign up (free)
2. Click **New → Web Service**
3. Connect your GitHub account and select the repository
4. Render will auto-detect the `render.yaml` config
5. Click **Deploy** — takes ~2 minutes
6. You get a URL like: `https://kharif25-awd-dashboard.onrender.com`

### That's it — share the link with anyone.

## Local development

```bash
pip install flask
python app.py
# Open http://localhost:5000
```
