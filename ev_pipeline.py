"""
ev_pipeline.py — shared ingest/clean/score logic for the NY EV site-selection project.

Used by BOTH the Jupyter notebook (interactively) and the Airflow DAG
(dags/ev_site_selection_dag.py). Every task is idempotent and file-cache-backed:
each step reads its inputs from / writes its outputs to WORKDIR, so Airflow tasks
pass data via the filesystem (no XCom payloads needed). Live sources fall back to
a clearly-labelled synthetic NY sample so a run never hard-fails.

Env vars:
  NREL_API_KEY                  -> NREL AFDC key (falls back to DEMO_KEY)
  CONED_HOSTING_CAPACITY_URL    -> ArcGIS layer url for feeder EV Load Capacity (optional)
  EV_WORKDIR                    -> working dir (default ./ev_work)
"""
from __future__ import annotations
import os, json, math, time
from pathlib import Path
import numpy as np
import pandas as pd
import requests

# ----------------------------------------------------------------- config
WORKDIR = Path(os.environ.get("EV_WORKDIR", "ev_work"))
WORKDIR.mkdir(parents=True, exist_ok=True)

SOCRATA_HOST = "https://data.ny.gov"
DAC_DATASET, EVREG_DATASET, CR_DATASET = "2e6c-s6fp", "3vp6-cxmr", "9wxk-hakb"
TARGET_COUNTIES = ["Bronx", "Kings", "New York", "Queens", "Richmond", "Westchester"]

WEIGHTS = {"equity_dac":0.25,"ev_demand":0.20,"traffic":0.15,
           "grid_capacity":0.15,"accessibility":0.10,"infra_gap":0.15}
STATION_RADIUS_KM = 2.0
TOP_N = 25
MIN_POP = 200

NREL_KEY = os.environ.get("NREL_API_KEY", "DEMO_KEY")
CONED_URL = os.environ.get("CONED_HOSTING_CAPACITY_URL", "")

def _p(name, ext="parquet"): return str(WORKDIR / f"{name}.{ext}")

def _save(df, name):
    try: df.to_parquet(_p(name), index=False)
    except Exception: df.to_csv(_p(name, "csv"), index=False)

def _load(name):
    if os.path.exists(_p(name)): return pd.read_parquet(_p(name))
    return pd.read_csv(_p(name, "csv"))

def _get(url, params=None, timeout=60):
    r = requests.get(url, params=params, timeout=timeout,
                     headers={"User-Agent": "cjf-ev-pipeline/1.0"})
    r.raise_for_status(); return r

def haversine_km(a, b, c, d):
    R=6371.0; p1,p2=math.radians(a),math.radians(c)
    x=math.sin(math.radians(c-a)/2)**2+math.cos(p1)*math.cos(p2)*math.sin(math.radians(d-b)/2)**2
    return 2*R*math.asin(math.sqrt(x))

def _centroid(feature):
    acc=[]
    def walk(x):
        if isinstance(x,(list,tuple)) and x and isinstance(x[0],(int,float)): acc.append((x[0],x[1]))
        elif isinstance(x,(list,tuple)):
            for y in x: walk(y)
    walk(feature.get("geometry",{}).get("coordinates",[]))
    if not acc: return (None,None)
    return (sum(a[1] for a in acc)/len(acc), sum(a[0] for a in acc)/len(acc))

def _rings(coords,out):
    if not coords: return
    if isinstance(coords[0][0],(int,float)): out.append(coords)
    else:
        for c in coords: _rings(c,out)

def _area_km2(feature):
    rings=[]; _rings(feature.get("geometry",{}).get("coordinates",[]),rings)
    if not rings: return np.nan
    lat0=_centroid(feature)[0]
    if lat0 is None: return np.nan
    R=6371.0; k=math.cos(math.radians(lat0)); tot=0.0
    for ring in rings:
        s=0.0
        for i in range(len(ring)-1):
            x1=math.radians(ring[i][0])*R*k; y1=math.radians(ring[i][1])*R
            x2=math.radians(ring[i+1][0])*R*k; y2=math.radians(ring[i+1][1])*R
            s+=x1*y2-x2*y1
        tot+=s/2.0
    return abs(tot)

def _find_col(cols, cands):
    low={c.lower():c for c in cols}
    for c in cands:
        if c in low: return low[c]
    for c in cols:
        if any(k in c.lower() for k in cands): return c
    return None

# ----------------------------------------------------------------- EXTRACT
def extract_dac(**_):
    where="county in(%s)" % ",".join("'%s'"%c for c in TARGET_COUNTIES)
    sel=["geoid","county","city_town","dac_designation","population_count","combined_score",
         "percentile_rank_combined","traffic_number_vehicles","traffic_truck_highways"]
    try:
        rows=_get(f"{SOCRATA_HOST}/resource/{DAC_DATASET}.json",
                  {"$select":",".join(sel),"$where":where,"$limit":60000}).json()
        gj=_get(f"{SOCRATA_HOST}/resource/{DAC_DATASET}.geojson",
                {"$select":"the_geom,geoid,dac_designation","$where":where,"$limit":60000}).json()
        df=pd.DataFrame(rows); src="LIVE"
    except Exception as e:
        print(f"[extract_dac] fallback synthetic ({type(e).__name__})")
        df,gj=_synth_dac(); src="SYNTHETIC"
    _save(df,"dac"); json.dump(gj,open(_p("dac_geo","json"),"w"))
    print(f"[extract_dac] {src} rows={len(df)}"); return src

def extract_evreg(**_):
    try:
        probe=_get(f"{SOCRATA_HOST}/resource/{EVREG_DATASET}.json",{"$limit":1}).json()
        ccol=_find_col(list(probe[0].keys()),["county"])
        rows=_get(f"{SOCRATA_HOST}/resource/{EVREG_DATASET}.json",
                  {"$select":f"{ccol} as county, count(1) as ev_registrations","$group":ccol,"$limit":5000}).json()
        df=pd.DataFrame(rows); df["ev_registrations"]=pd.to_numeric(df["ev_registrations"],errors="coerce")
        df["county"]=df["county"].astype(str).str.title().str.strip(); src="LIVE"
    except Exception as e:
        print(f"[extract_evreg] fallback synthetic ({type(e).__name__})")
        base={"Bronx":9000,"Kings":21000,"New York":15000,"Queens":24000,"Richmond":8000,"Westchester":26000}
        df=pd.DataFrame([{"county":c,"ev_registrations":v} for c,v in base.items()]); src="SYNTHETIC"
    df=df[df["county"].isin(TARGET_COUNTIES)]; _save(df,"evreg")
    print(f"[extract_evreg] {src}"); return src

def extract_stations(**_):
    try:
        data=_get("https://developer.nrel.gov/api/alt-fuel-stations/v1.json",
                  {"api_key":NREL_KEY,"fuel_type":"ELEC","state":"NY","status":"E","access":"public","limit":"all"}).json()
        df=pd.DataFrame([{"lat":s.get("latitude"),"lon":s.get("longitude")} for s in data.get("fuel_stations",[])]); src="LIVE"
    except Exception as e:
        print(f"[extract_stations] fallback synthetic ({type(e).__name__})")
        gj=json.load(open(_p("dac_geo","json"))); rng=np.random.default_rng(11); rows=[]
        for f in rng.choice(gj["features"],size=min(180,len(gj["features"])),replace=False):
            la,lo=_centroid(f); rows.append({"lat":la,"lon":lo})
        df=pd.DataFrame(rows); src="SYNTHETIC"
    df["lat"]=pd.to_numeric(df["lat"],errors="coerce"); df["lon"]=pd.to_numeric(df["lon"],errors="coerce")
    _save(df.dropna(),"stations"); print(f"[extract_stations] {src} n={len(df)}"); return src

def extract_grid(**_):
    if not CONED_URL:
        print("[extract_grid] no CONED_HOSTING_CAPACITY_URL -> proxy will be used")
        _save(pd.DataFrame(columns=["lat","lon","ev_mw"]),"grid"); return "PROXY"
    try:
        feats,off=[],0
        while len(feats)<20000:
            js=_get(CONED_URL.rstrip("/")+"/query",
                    {"where":"1=1","outFields":"*","f":"geojson","resultRecordCount":1000,"resultOffset":off,"outSR":4326}).json()
            b=js.get("features",[]); feats+=b
            if not b or len(b)<1000: break
            off+=1000
        field=(os.environ.get("CONED_EV_CAPACITY_FIELD")
               or _find_col(list((feats[0].get("properties",{}) or {}).keys()),
                        ["ev_headroom","ev_load","ev_capacity","ev_load_capacity","headroom","hosting","capacity","mw"]))
        rows=[]
        for f in feats:
            la,lo=_centroid(f)
            if la is None: continue
            rows.append({"lat":la,"lon":lo,"ev_mw":pd.to_numeric(pd.Series([f["properties"].get(field)]),errors="coerce").iloc[0]})
        df=pd.DataFrame(rows); _save(df,"grid"); print(f"[extract_grid] LIVE feeders={len(df)} field={field}"); return "LIVE"
    except Exception as e:
        print(f"[extract_grid] fallback proxy ({type(e).__name__})")
        _save(pd.DataFrame(columns=["lat","lon","ev_mw"]),"grid"); return "PROXY"

def extract_charge_ready(**_):
    try:
        probe=_get(f"{SOCRATA_HOST}/resource/{CR_DATASET}.json",{"$limit":1}).json()
        cols=list(probe[0].keys())
        latc=_find_col(cols,["latitude","lat"]); lonc=_find_col(cols,["longitude","lon"])
        geoc=_find_col(cols,["georeference","location","point"])
        raw=_get(f"{SOCRATA_HOST}/resource/{CR_DATASET}.json",{"$limit":50000}).json(); df=pd.DataFrame(raw)
        def xy(r):
            if latc and lonc and pd.notna(r.get(latc)) and pd.notna(r.get(lonc)):
                return pd.Series([pd.to_numeric(r.get(latc),errors="coerce"),pd.to_numeric(r.get(lonc),errors="coerce")])
            g=r.get(geoc)
            if isinstance(g,dict) and g.get("coordinates"): return pd.Series([g["coordinates"][1],g["coordinates"][0]])
            return pd.Series([np.nan,np.nan])
        df[["lat","lon"]]=df.apply(xy,axis=1); df=df[["lat","lon"]].dropna(); src="LIVE"
    except Exception as e:
        print(f"[extract_charge_ready] fallback synthetic ({type(e).__name__})")
        gj=json.load(open(_p("dac_geo","json"))); rng=np.random.default_rng(23); rows=[]
        for f in rng.choice(gj["features"],size=min(45,len(gj["features"])),replace=False):
            la,lo=_centroid(f); rows.append({"lat":la,"lon":lo})
        df=pd.DataFrame(rows); src="SYNTHETIC"
    _save(df,"charge_ready"); print(f"[extract_charge_ready] {src} n={len(df)}"); return src

# ----------------------------------------------------------------- TRANSFORM
def transform_integrate(**_):
    dac=_load("dac"); gj=json.load(open(_p("dac_geo","json")))
    ev=_load("evreg"); st=_load("stations"); grid=_load("grid")
    for c in ["population_count","combined_score","percentile_rank_combined",
              "traffic_number_vehicles","traffic_truck_highways"]:
        if c in dac.columns: dac[c]=pd.to_numeric(dac[c],errors="coerce")
    dac["geoid"]=dac["geoid"].astype(str).str.replace(r"\.0$","",regex=True).str.zfill(11)
    dac["is_dac"]=dac["dac_designation"].astype(str).str.strip().eq("Designated as DAC")
    area={}; cent={}
    for f in gj["features"]:
        g=str(f["properties"].get("geoid","")).zfill(11); area[g]=_area_km2(f); cent[g]=_centroid(f)
    dac["area_km2"]=dac["geoid"].map(area)
    dac["lat"]=dac["geoid"].map(lambda g:cent.get(g,(None,None))[0])
    dac["lon"]=dac["geoid"].map(lambda g:cent.get(g,(None,None))[1])
    dac["pop_density"]=dac["population_count"]/dac["area_km2"].replace(0,np.nan)
    dac=dac[dac["population_count"].fillna(0)>=MIN_POP].drop_duplicates("geoid").reset_index(drop=True)
    for c in ["area_km2","pop_density","lat","lon"]:
        dac[c]=dac[c].fillna(dac.groupby("county")[c].transform("median"))
    # chargers within radius
    sl,so=st["lat"].to_numpy(),st["lon"].to_numpy()
    def cnt(la,lo):
        if pd.isna(la) or not len(sl): return 0
        dl=STATION_RADIUS_KM/111.0; dg=STATION_RADIUS_KM/(111.0*max(math.cos(math.radians(la)),1e-3))
        msk=(np.abs(sl-la)<=dl)&(np.abs(so-lo)<=dg)
        return int(sum(haversine_km(la,lo,a,b)<=STATION_RADIUS_KM for a,b in zip(sl[msk],so[msk]))) if msk.any() else 0
    dac["stations_within_r"]=[cnt(a,b) for a,b in zip(dac["lat"],dac["lon"])]
    # grid headroom
    if len(grid) and grid["ev_mw"].notna().any():
        gl,go,gm=grid["lat"].to_numpy(),grid["lon"].to_numpy(),pd.to_numeric(grid["ev_mw"],errors="coerce").to_numpy()
        def nmw(la,lo):
            d=[haversine_km(la,lo,a,b) for a,b in zip(gl,go)]; return float(gm[int(np.argmin(d))])
        dac["grid_headroom_mw"]=[nmw(a,b) for a,b in zip(dac["lat"],dac["lon"])]
    else:
        dac["grid_headroom_mw"]=np.nan
    cp=dac.groupby("county",as_index=False)["population_count"].sum().rename(columns={"population_count":"county_pop"})
    ev=ev.merge(cp,on="county",how="right")
    ev["ev_registrations"]=pd.to_numeric(ev["ev_registrations"],errors="coerce").fillna(0)
    ev["ev_per_1k"]=1000*ev["ev_registrations"]/ev["county_pop"].replace(0,np.nan)
    master=dac.merge(ev[["county","ev_per_1k"]],on="county",how="left")
    _save(master,"master"); print(f"[transform] master rows={len(master)}"); return len(master)

# ----------------------------------------------------------------- SCORE
def _minmax(s):
    s=s.astype(float); lo,hi=s.min(),s.max()
    return pd.Series(0.5,index=s.index) if hi-lo<1e-12 else (s-lo)/(hi-lo)

def score(**_):
    m=_load("master")
    m["equity_dac"]=pd.to_numeric(m["percentile_rank_combined"],errors="coerce"); m["equity_dac"]=m["equity_dac"].fillna(m["equity_dac"].median())
    m["ev_demand"]=m["ev_per_1k"].fillna(m["ev_per_1k"].median())
    tr=pd.to_numeric(m.get("traffic_number_vehicles"),errors="coerce")
    m["traffic"]=tr.fillna(tr.median() if tr.notna().any() else 0.5)
    if m["grid_headroom_mw"].notna().any():
        m["grid_capacity"]=m["grid_headroom_mw"].fillna(m["grid_headroom_mw"].median())
    else:
        d=_minmax(m["pop_density"]); m["grid_capacity"]=1-d
    m["accessibility"]=m["pop_density"].fillna(m["pop_density"].median())
    m["infra_gap"]=1.0/(1.0+m["stations_within_r"])
    for c in WEIGHTS: m[c+"_n"]=_minmax(m[c])
    m["suitability_score"]=sum(m[c+"_n"]*w for c,w in WEIGHTS.items())*100.0
    m["rank"]=m["suitability_score"].rank(ascending=False,method="first").astype(int)
    _save(m,"scored"); print(f"[score] scored={len(m)} range {m['suitability_score'].min():.1f}-{m['suitability_score'].max():.1f}"); return len(m)

def export(**_):
    m=_load("scored"); outd=WORKDIR/"outputs"; outd.mkdir(exist_ok=True)
    top=m.sort_values("suitability_score",ascending=False).head(TOP_N)
    m.to_csv(outd/"master_integrated_dataset.csv",index=False)
    top.to_csv(outd/"ranked_site_recommendations.csv",index=False)
    m[m["is_dac"]].sort_values("suitability_score",ascending=False).head(TOP_N).to_csv(outd/"ranked_site_recommendations_DAC.csv",index=False)
    json.dump({"generated_at":time.strftime("%Y-%m-%d %H:%M:%S"),"n_tracts":int(len(m)),
               "weights":WEIGHTS,"top_n":TOP_N}, open(outd/"run_manifest.json","w"), indent=2)
    print(f"[export] wrote {outd}"); return str(outd)

def run_all():
    extract_dac(); extract_evreg(); extract_stations(); extract_grid(); extract_charge_ready()
    transform_integrate(); score(); export()

# minimal synthetic DAC used when live extract fails (keeps pipeline runnable offline)
def _synth_dac(n=120, seed=42):
    rng=np.random.default_rng(seed)
    centers={"Bronx":(40.85,-73.87),"Kings":(40.65,-73.95),"New York":(40.78,-73.97),
             "Queens":(40.70,-73.82),"Richmond":(40.58,-74.15),"Westchester":(41.12,-73.79)}
    fips={"Bronx":"005","Kings":"047","New York":"061","Queens":"081","Richmond":"085","Westchester":"119"}
    recs,feats=[],[]
    for county,(clat,clon) in centers.items():
        for i in range(n):
            gid=f"36{fips[county]}{i:06d}"; lat=clat+rng.normal(0,0.06); lon=clon+rng.normal(0,0.06)
            dac=rng.random()<0.5; pct=float(np.clip(rng.beta(3,2) if dac else rng.beta(2,3),0,1))
            recs.append(dict(geoid=gid,county=county,city_town="New York city",
                dac_designation=("Designated as DAC" if dac else "Not Designated as DAC"),
                population_count=int(np.clip(rng.normal(3800,1200),0,12000)),
                combined_score=float(30+pct*110),percentile_rank_combined=pct,
                traffic_number_vehicles=float(np.clip(rng.beta(2,2),0,1)),
                traffic_truck_highways=float(np.clip(rng.beta(2,3),0,1))))
            d=0.012+rng.random()*0.02
            feats.append(dict(type="Feature",properties=dict(geoid=gid,dac_designation=recs[-1]["dac_designation"]),
                geometry=dict(type="Polygon",coordinates=[[[lon-d,lat-d],[lon+d,lat-d],[lon+d,lat+d],[lon-d,lat+d],[lon-d,lat-d]]])))
    return pd.DataFrame(recs), dict(type="FeatureCollection",features=feats)

if __name__ == "__main__":
    run_all()
