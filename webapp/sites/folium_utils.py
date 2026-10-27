"""
folium_utils.py — builds the live suitability map served at /map/.

Mirrors the notebook's Section 10 map (choropleth of every scored tract +
top-N recommended sites + existing chargers), but reads from the database
(synced by `sync_sites`) instead of an in-memory dataframe, and swaps the
notebook's single-shot "Find" button for a debounced autocomplete dropdown
backed by this app's own /api/geocode/ and /api/nearest/ endpoints.
"""
from __future__ import annotations

import json

import branca.colormap as cm
import folium
from branca.element import MacroElement
from folium.plugins import MarkerCluster
from jinja2 import Template


class AddressAutocomplete(MacroElement):
    """Top-right search control: type an address, pick a live suggestion,
    see the nearest recommended site + whether it's within the coverage radius."""

    def __init__(self, coverage_radius_km: float, geocode_url: str, nearest_url: str):
        super().__init__()
        self._name = "AddressAutocomplete"
        self.radius = coverage_radius_km
        self.geocode_url = geocode_url
        self.nearest_url = nearest_url
        self._template = Template(r"""
{% macro script(this, kwargs) %}
var MAP = {{ this._parent.get_name() }};
var THR = {{ this.radius }};
var GEOCODE_URL = {{ this.geocode_url|tojson }};
var NEAREST_URL = {{ this.nearest_url|tojson }};

var ctl = L.control({position: 'topright'});
ctl.onAdd = function () {
  var d = L.DomUtil.create('div', 'ev-search');
  d.innerHTML =
    '<div class="ev-search-box">' +
    '  <input id="ev-addr" type="text" placeholder="Type an NY address…" autocomplete="off"/>' +
    '  <div id="ev-suggest" class="ev-suggest"></div>' +
    '  <div id="ev-result" class="ev-result"></div>' +
    '</div>';
  L.DomEvent.disableClickPropagation(d);
  L.DomEvent.disableScrollPropagation(d);
  return d;
};
ctl.addTo(MAP);

setTimeout(function () {
  var input = document.getElementById('ev-addr');
  var suggestBox = document.getElementById('ev-suggest');
  var resultBox = document.getElementById('ev-result');
  var marker = null, line = null, debounceTimer = null;

  // Suggestion labels come from a third-party geocoder (Photon/OSM data) and
  // must never be inserted into innerHTML/bindPopup unescaped.
  function esc(s) {
    return String(s == null ? '' : s).replace(/[&<>"']/g, function (c) {
      return {'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c];
    });
  }

  function clearSuggestions() { suggestBox.innerHTML = ''; suggestBox.style.display = 'none'; }

  function renderSuggestions(items) {
    if (!items.length) { suggestBox.innerHTML = '<div class="ev-suggest-empty">No matches</div>'; suggestBox.style.display = 'block'; return; }
    suggestBox.innerHTML = items.map(function (it, i) {
      return '<div class="ev-suggest-item" data-i="' + i + '">' + esc(it.label) + '</div>';
    }).join('');
    suggestBox.style.display = 'block';
    Array.prototype.forEach.call(suggestBox.querySelectorAll('.ev-suggest-item'), function (el) {
      el.onclick = function () { selectPlace(items[parseInt(el.dataset.i, 10)]); };
    });
  }

  function selectPlace(place) {
    input.value = place.label;
    clearSuggestions();
    resultBox.innerHTML = '<span class="ev-muted">Locating nearest recommended site…</span>';
    if (marker) MAP.removeLayer(marker);
    if (line) MAP.removeLayer(line);
    marker = L.marker([place.lat, place.lon]).addTo(MAP);
    MAP.setView([place.lat, place.lon], 13);

    fetch(NEAREST_URL + '?lat=' + place.lat + '&lon=' + place.lon + '&radius_km=' + THR)
      .then(function (r) { return r.json(); })
      .then(function (site) {
        if (!site || !site.rank) { resultBox.innerHTML = '<span class="ev-bad">No recommended sites loaded yet — run a sync.</span>'; return; }
        var near = site.distance_km <= THR;
        line = L.polyline([[place.lat, place.lon], [site.lat, site.lon]],
          {color: near ? '#16a34a' : '#dc2626', dashArray: '6,6', weight: 3}).addTo(MAP);
        marker.bindPopup(
          '<b>' + esc(place.label) + '</b><br/>Nearest recommended site: #' + esc(site.rank) +
          ' (' + esc(site.county) + (site.is_dac ? ', DAC' : '') + ')<br/>' +
          site.distance_km.toFixed(2) + ' km away · score ' + site.suitability_score.toFixed(1)
        ).openPopup();
        resultBox.innerHTML =
          '<span class="' + (near ? 'ev-good' : 'ev-bad') + '">' + (near ? '✓ within ' : '✗ not within ') + THR + ' km</span>' +
          ' of site #' + esc(site.rank) + ' (' + esc(site.county) + ') — ' + site.distance_km.toFixed(2) + ' km';
      })
      .catch(function () { resultBox.innerHTML = '<span class="ev-bad">Lookup failed.</span>'; });
  }

  input.addEventListener('input', function () {
    var q = input.value.trim();
    clearTimeout(debounceTimer);
    if (q.length < 3) { clearSuggestions(); return; }
    debounceTimer = setTimeout(function () {
      fetch(GEOCODE_URL + '?q=' + encodeURIComponent(q))
        .then(function (r) { return r.json(); })
        .then(function (j) { renderSuggestions(j.results || []); })
        .catch(function () { clearSuggestions(); });
    }, 300);
  });

  document.addEventListener('click', function (e) {
    if (!suggestBox.contains(e.target) && e.target !== input) clearSuggestions();
  });
}, 400);
{% endmacro %}
""")


def build_suitability_map(top_sites, tract_scores, dac_geojson, coverage_radius_km=2.0,
                           chargers=None, geocode_url="/api/geocode/", nearest_url="/api/nearest/"):
    """top_sites: list of SiteRecommendation-like dicts (rank, geoid, county, lat, lon, ...)
    tract_scores: {geoid: suitability_score} for the choropleth
    dac_geojson: FeatureCollection of tract boundaries
    chargers: optional list of {lat, lon} existing charger points
    """
    if top_sites:
        center = [
            sum(s["lat"] for s in top_sites) / len(top_sites),
            sum(s["lon"] for s in top_sites) / len(top_sites),
        ]
    else:
        center = [40.75, -73.95]

    # "cartodbpositron" now resolves to CARTO's basemap CDN, which gates anonymous
    # use behind an API key (returns a watermarked "API key required" tile instead
    # of a real basemap). Raw OpenStreetMap tile.openstreetmap.org is against OSM's
    # tile usage policy for app embedding and gets silently blocked. Esri's public
    # ArcGIS Online basemaps are free, keyless, and safe for this use.
    fmap = folium.Map(location=center, zoom_start=10, tiles=None)
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/Canvas/World_Light_Gray_Base/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ",
        name="Esri Gray Canvas", control=False,
    ).add_to(fmap)

    if tract_scores:
        vals = list(tract_scores.values())
        vmin, vmax = min(vals), max(vals)
        colormap = cm.linear.YlOrRd_09.scale(vmin, vmax)
        colormap.caption = "EV charging suitability score"
        colormap.add_to(fmap)

        def style_fn(feat):
            g = str(feat["properties"].get("geoid", "")).zfill(11)
            v = tract_scores.get(g)
            return {
                "fillColor": colormap(v) if v is not None else "#cccccc",
                "color": "#666666", "weight": 0.3,
                "fillOpacity": 0.7 if v is not None else 0.05,
            }

        folium.GeoJson(
            dac_geojson, name="Suitability (tract)", style_function=style_fn,
            tooltip=folium.GeoJsonTooltip(fields=["geoid"], aliases=["Tract:"]),
        ).add_to(fmap)

    top_layer = folium.FeatureGroup(name=f"Top {len(top_sites)} recommended sites").add_to(fmap)
    for s in top_sites:
        folium.CircleMarker(
            [s["lat"], s["lon"]], radius=6, color="#08306b", fill=True, fill_opacity=0.9,
            popup=folium.Popup(
                f"<b>Rank #{s['rank']}</b><br>Tract {s['geoid']}<br>{s['county']}<br>"
                f"Score {s['suitability_score']:.1f}<br>DAC {bool(s['is_dac'])}<br>"
                f"EV/1k {s.get('ev_per_1k') or 0:.1f}<br>"
                f"Chargers ≤{coverage_radius_km}km {s.get('stations_within_r') or 0}",
                max_width=260,
            ),
        ).add_to(top_layer)

    if chargers:
        st_layer = folium.FeatureGroup(name="Existing chargers", show=False).add_to(fmap)
        mc = MarkerCluster().add_to(st_layer)
        for c in chargers:
            folium.CircleMarker([c["lat"], c["lon"]], radius=3, color="#2b8cbe",
                                 fill=True, fill_opacity=0.6).add_to(mc)

    fmap.get_root().header.add_child(folium.Element(_MAP_CSS))
    fmap.add_child(AddressAutocomplete(coverage_radius_km, geocode_url, nearest_url))
    folium.LayerControl(collapsed=False).add_to(fmap)
    return fmap


_MAP_CSS = """
<style>
.ev-search-box {
  background: #ffffff; padding: 10px 12px; border-radius: 10px;
  box-shadow: 0 2px 10px rgba(0,0,0,0.18); font: 13px/1.4 system-ui, -apple-system, sans-serif;
  width: 240px;
}
.ev-search-box input {
  width: 100%; box-sizing: border-box; padding: 7px 9px; border: 1px solid #d7dbe0;
  border-radius: 7px; font-size: 13px; outline: none;
}
.ev-search-box input:focus { border-color: #2563eb; box-shadow: 0 0 0 2px rgba(37,99,235,0.15); }
.ev-suggest {
  display: none; margin-top: 4px; max-height: 190px; overflow-y: auto;
  border: 1px solid #e5e7eb; border-radius: 7px; background: #fff;
}
.ev-suggest-item { padding: 7px 9px; cursor: pointer; border-bottom: 1px solid #f1f3f5; }
.ev-suggest-item:last-child { border-bottom: none; }
.ev-suggest-item:hover { background: #eff6ff; }
.ev-suggest-empty { padding: 7px 9px; color: #9ca3af; }
.ev-result { margin-top: 6px; color: #374151; }
.ev-muted { color: #9ca3af; }
.ev-good { color: #16a34a; font-weight: 600; }
.ev-bad { color: #dc2626; font-weight: 600; }
</style>
"""
