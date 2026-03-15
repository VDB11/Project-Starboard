const DISASTER_ICONS = { EQ: "🌐", TC: "🌀", FL: "🌧️", VO: "🌋", DR: "☀️" };

let disasterLayers = [];
let hoverPopup = null;
let clickedPopupOpen = false;

function clearDisasterLayers() {
    disasterLayers.forEach(l => map.removeLayer(l));
    disasterLayers = [];
    if (hoverPopup) {
        map.closePopup(hoverPopup);
        hoverPopup = null;
    }
    clickedPopupOpen = false;
}

function buildPopupHTML(ev) {
    const icon = DISASTER_ICONS[ev.eventtype] || "⚠️";
    const alertColor = ev.color || "#43A047";
    const alertLabel = ev.alertlevel
        ? ev.alertlevel.charAt(0).toUpperCase() + ev.alertlevel.slice(1)
        : "Unknown";

    return `
        <div style="font-family:'Roboto Slab',serif; min-width:210px; max-width:290px;">
            <div style="background:${alertColor}; color:#fff; padding:8px 12px; border-radius:6px 6px 0 0; display:flex; align-items:center; gap:8px;">
                <span style="font-size:18px;">${icon}</span>
                <div>
                    <div style="font-weight:700; font-size:13px;">${ev.eventtype_name}</div>
                    <div style="font-size:11px; opacity:0.9;">${alertLabel} Alert</div>
                </div>
            </div>
            <div style="padding:10px 12px; background:#fff; border-radius:0 0 6px 6px; border:1px solid #eee; border-top:none;">
                <div style="font-weight:600; font-size:13px; color:#222; margin-bottom:6px;">${ev.name || "—"}</div>
                ${ev.country ? `<div style="font-size:12px; color:#555; margin-bottom:4px;">📍 ${ev.country}</div>` : ""}
                ${ev.fromdate ? `<div style="font-size:12px; color:#555; margin-bottom:4px;">📅 From: ${ev.fromdate}</div>` : ""}
                ${ev.severitytext ? `<div style="font-size:12px; color:#555; margin-bottom:4px;">⚡ ${ev.severitytext}</div>` : ""}

            </div>
        </div>
    `;
}

function renderDisasterEvents(routeEvents, portEvents) {
    clearDisasterLayers();

    const allEvents = [
        ...routeEvents.map(e => ({ ...e, _source: "route" })),
        ...portEvents.map(e => ({ ...e, _source: "port" }))
    ];

    const seen = new Set();

    allEvents.forEach(ev => {
        const key = `${ev.eventid}-${ev.episodeid}`;
        if (seen.has(key)) return;
        seen.add(key);

        const color = ev.color || "#43A047";
        const popupHTML = buildPopupHTML(ev);

        if (!ev.geojson || !ev.geojson.features) return;

        ev.geojson.features.forEach(feature => {
            const geomType = feature.geometry && feature.geometry.type;
            if (!geomType) return;

            let layer;
            if (geomType === "Point") {
                const [lon, lat] = feature.geometry.coordinates;
                layer = L.circleMarker([lat, lon], {
                    radius: 8,
                    color: color,
                    fillColor: color,
                    fillOpacity: 0.7,
                    weight: 2
                });
            } else {
                layer = L.geoJSON(feature, {
                    style: {
                        color:       color,
                        fillColor:   color,
                        fillOpacity: 0.25,
                        weight:      2,
                        dashArray:   ev._source === "port" ? "5,4" : null
                    }
                });
            }

            layer.on("mouseover", function (e) {
                if (clickedPopupOpen) return;
                const latlng = e.latlng || (layer.getBounds ? layer.getBounds().getCenter() : null);
                if (!latlng) return;
                if (hoverPopup) map.closePopup(hoverPopup);
                hoverPopup = L.popup({ autoClose: true, closeOnClick: true, closeButton: false })
                    .setLatLng(latlng)
                    .setContent(popupHTML)
                    .openOn(map);
            });

            layer.on("mouseout", function () {
                if (clickedPopupOpen) return;
                if (hoverPopup) {
                    map.closePopup(hoverPopup);
                    hoverPopup = null;
                }
            });

            layer.on("click", function (e) {
                L.DomEvent.stopPropagation(e);
                if (hoverPopup) {
                    map.closePopup(hoverPopup);
                    hoverPopup = null;
                }
                clickedPopupOpen = true;
                const latlng = e.latlng || (layer.getBounds ? layer.getBounds().getCenter() : null);
                L.popup({ autoClose: true, closeOnClick: true, closeButton: false })
                    .setLatLng(latlng)
                    .setContent(popupHTML)
                    .openOn(map);
            });

            layer.addTo(map);
            disasterLayers.push(layer);
        });
    });

    map.on("click", function () {
        clickedPopupOpen = false;
    });

    map.on("popupclose", function () {
        clickedPopupOpen = false;
    });

    document.addEventListener("keydown", function (e) {
        if (e.key === "Escape") {
            map.closePopup();
            clickedPopupOpen = false;
        }
    });
}

function toggleDisasterLayers(visible) {
    disasterLayers.forEach(l => {
        if (visible) {
            if (!map.hasLayer(l)) map.addLayer(l);
        } else {
            if (map.hasLayer(l)) map.removeLayer(l);
        }
    });
}

window.clearDisasterLayers  = clearDisasterLayers;
window.renderDisasterEvents = renderDisasterEvents;
window.toggleDisasterLayers = toggleDisasterLayers;