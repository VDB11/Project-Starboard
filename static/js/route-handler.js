function unwrapCoordinates(coords, startNearLon) {
    if (coords.length === 0) return coords;
    let firstLon = coords[0][1];
    if (startNearLon !== undefined) {
        while (firstLon - startNearLon > 180) firstLon -= 360;
        while (startNearLon - firstLon > 180) firstLon += 360;
    }
    const result = [[coords[0][0], firstLon]];
    for (let i = 1; i < coords.length; i++) {
        let [lat, lon] = coords[i];
        let prevLon = result[i - 1][1];
        while (lon - prevLon > 180) lon -= 360;
        while (prevLon - lon > 180) lon += 360;
        result.push([lat, lon]);
    }
    return result;
}

const SEGMENT_COLORS = ["#0066ff", "#ff6600", "#00cc44", "#cc00ff", "#ff0033", "#00cccc", "#ffcc00"];

document.getElementById('calculate-route').addEventListener('click', async function() {
    const stops = getStopsPayload();

    document.getElementById('loading').style.display = 'block';
    clearMap();

    try {
        const res = await fetch('/api/route', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ stops })
        });

        const data = await res.json();
        document.getElementById('loading').style.display = 'none';

        if (data.error) {
            alert('Error: ' + data.error);
            return;
        }

        const allCoords = [];
        let lastLon = undefined;

        data.segments.forEach((segment, i) => {
            const color = i === 0 ? '#0066ff' : SEGMENT_COLORS[i % SEGMENT_COLORS.length];
            const unwrapped = unwrapCoordinates(segment.coordinates, lastLon);
            lastLon = unwrapped[unwrapped.length - 1][1];

            const line = L.polyline(unwrapped, {
                color: color,
                weight: 4,
                opacity: 0.8,
                lineCap: 'round',
                lineJoin: 'round'
            }).addTo(map);

            line.bindTooltip(
                `<div style="font-family:'Roboto Slab',serif; font-size:13px; padding:4px 2px;">
                    <strong style="font-size:14px; color:#111;">${segment.from.port_name}</strong>
                    <span style="color:#4facfe; margin:0 6px; font-size:16px;">⟶</span>
                    <strong style="font-size:14px; color:#111;">${segment.to.port_name}</strong>
                    <div style="color:#333; font-size:12px; font-weight:700; margin-top:4px;">${segment.length.toLocaleString()} nautical miles</div>
                </div>`,
                { sticky: true, direction: 'top', opacity: 1.0 }
            );

            line.on('mouseover', function(e) { line.openTooltip(e.latlng); });
            line.on('mouseout',  function()  { line.closeTooltip(); });

            window.routeLayers.push(line);
            allCoords.push(...unwrapped);

            if (i === 0) {
                addPortMarker(segment.from, 'green', 'Origin', unwrapped[0][1]);
            }

            if (i === data.segments.length - 1) {
                const isRoundTrip = segment.to.port_name === data.segments[0].from.port_name;
                if (isRoundTrip) {
                    addPortMarker(segment.to, 'green', 'Origin / Destination', unwrapped[unwrapped.length - 1][1]);
                } else {
                    addPortMarker(segment.to, 'red', 'Destination', unwrapped[unwrapped.length - 1][1]);
                }
            } else {
                addPortMarker(segment.to, 'orange', `Stop ${i + 1}`, unwrapped[unwrapped.length - 1][1]);
            }
        });

        if (allCoords.length > 0) {
            map.fitBounds(L.latLngBounds(allCoords), { padding: [30, 30] });
        }

        renderRouteSummary(data.segments, data.total_length);
        const toggleRow = document.getElementById('disaster-toggle-row');
        if (toggleRow) toggleRow.style.display = 'flex';
        fetchAndRenderDisasters(data.segments);

    } catch (err) {
        document.getElementById('loading').style.display = 'none';
        alert('Failed to calculate route: ' + err.message);
    }
});

function renderRouteSummary(segments, totalLength) {
    const container = document.getElementById('route-summary');
    if (!container) return;

    const segRows = segments.map((seg, i) => {
        const color = SEGMENT_COLORS[i % SEGMENT_COLORS.length];
        const distStr = segments.length > 1
            ? `<span style="font-size:12px; color:#aaa; margin-left:auto; white-space:nowrap;">${seg.length.toLocaleString()} nmi</span>`
            : '';
        return `<div style="display:flex; align-items:center; gap:10px; padding:8px 0; border-bottom:1px solid rgba(255,255,255,0.07);">
            <span style="display:inline-block; width:12px; height:4px; background:${color}; border-radius:2px; flex-shrink:0;"></span>
            <span style="font-size:14px; color:#fff; font-weight:500;">${seg.from.port_name} <span style="color:#4facfe;">→</span> ${seg.to.port_name}</span>
            ${distStr}
        </div>`;
    }).join('');

    container.innerHTML = `
        <div style="display:flex; justify-content:space-between; align-items:center; padding:6px 0 12px; border-bottom:1px solid rgba(255,255,255,0.1); margin-bottom:6px;">
            <span style="font-size:13px; color:#aaa; text-transform:uppercase; letter-spacing:0.5px;">Total Distance</span>
            <span style="font-size:20px; font-weight:700; color:#4facfe;">${totalLength.toLocaleString()} <span style="font-size:13px; font-weight:400; color:#aaa;">nmi</span></span>
        </div>
        ${segRows}
    `;

    const section = document.getElementById('route-summary-section');
    if (section) section.style.display = 'block';
}

const _disasterCache = {};

function _routeKey(segments) {
    return segments.map(s => `${s.from.port_name}|${s.to.port_name}`).join('>>');
}

async function fetchAndRenderDisasters(segments) {
    try {
        const key = _routeKey(segments);

        if (!_disasterCache[key]) {
            const res = await fetch('/api/disasters', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ segments })
            });
            const data = await res.json();
            if (data.error) {
                console.warn('Disasters API error:', data.error);
                return;
            }
            _disasterCache[key] = {
                route_events: data.route_events || [],
                port_events:  data.port_events  || []
            };
        }

        const cached = _disasterCache[key];

        window._getPortDisasters = function(port) {
            return cached.port_events.filter(ev =>
                Math.abs(ev.latitude - port.lat) < 2.5 &&
                Math.abs(ev.longitude - port.lon) < 2.5
            );
        };

        renderDisasterEvents(cached.route_events, []);
        renderDisasterSidebar(cached.route_events);

    } catch (err) {
        console.warn('Failed to fetch disasters:', err.message);
    }
}

function renderDisasterSidebar(routeEvents) {
    const container = document.getElementById('disaster-alerts');
    if (!container) return;

    const section = document.getElementById('route-summary-section');

    if (routeEvents.length === 0) {
        container.innerHTML = `<div style="display:flex; align-items:center; gap:10px; padding:10px 12px; background:rgba(255,255,255,0.05); border-radius:6px;">
            <span style="font-size:18px;">✅</span>
            <span style="font-size:14px; color:#aaa;">No active disaster events on this route.</span>
        </div>`;
        return;
    }

    const count = routeEvents.length;

    container.innerHTML = `
        <div style="display:flex; align-items:center; gap:12px; padding:12px 14px; background:rgba(255,255,255,0.06); border-left:4px solid #4facfe; border-radius:6px;">
            <span style="font-size:22px;">⚠️</span>
            <div style="font-size:16px; font-weight:700; color:#fff;">${count} Active Event${count > 1 ? 's' : ''} on Route</div>
        </div>`;
}

function buildPortPopup(port, label, portDisasters) {
    const ICONS = { EQ: "🌍", TC: "🌀", FL: "🌊", VO: "🌋", DR: "☀️" };
    const harborSize  = port.harbor_size  || "—";
    const harborType  = port.harbor_type  || "—";
    const countryCode = port.country_code || "—";

    const roleColors = { Origin: "#4CAF50", Destination: "#F44336" };
    const headerColor = roleColors[label] || "#FF9800";

    let disasterHTML = "";
    if (portDisasters && portDisasters.length > 0) {
        const sorted = [...portDisasters].sort((a, b) => {
            const o = { red: 0, orange: 1, green: 2 };
            return (o[a.alertlevel] ?? 3) - (o[b.alertlevel] ?? 3);
        });
        const rows = sorted.map(ev => {
            const color = ev.color || "#43A047";
            const icon  = ICONS[ev.eventtype] || "⚠️";
            return `
                <div style="display:flex; align-items:center; gap:10px; padding:8px 0; border-bottom:1px solid #f0f0f0;">
                    <span style="font-size:18px; line-height:1;">${icon}</span>
                    <div style="flex:1; min-width:0;">
                        <div style="font-size:14px; font-weight:600; color:#222;">${ev.name || ev.eventtype_name}</div>
                        <div style="font-size:12px; color:#888; margin-top:2px;">${ev.fromdate || ""}</div>
                    </div>
                    <span style="width:10px; height:10px; border-radius:50%; background:${color}; flex-shrink:0;"></span>
                </div>`;
        }).join("");
        disasterHTML = `
            <div style="margin-top:12px; padding-top:10px; border-top:2px solid #f0f0f0;">
                <div style="font-size:12px; font-weight:700; color:#e53935; text-transform:uppercase; letter-spacing:0.5px; margin-bottom:8px;">⚠ Active Alerts Nearby</div>
                ${rows}
            </div>`;
    }

    return `
        <div style="font-family:'Roboto Slab',serif; min-width:240px; max-width:300px;">
            <div style="background:${headerColor}; padding:12px 16px; margin:-1px -1px 0 -1px; border-radius:6px 6px 0 0;">
                <div style="font-size:17px; font-weight:700; color:#fff;">${port.port_name}</div>
                <div style="font-size:13px; color:rgba(255,255,255,0.85); margin-top:3px;">${label} · ${countryCode}</div>
            </div>
            <div style="padding:14px 16px;">
                <div style="display:grid; grid-template-columns:1fr 1fr; gap:12px;">
                    <div>
                        <div style="font-size:11px; color:#999; text-transform:uppercase; letter-spacing:0.4px; margin-bottom:4px;">Harbor Size</div>
                        <div style="font-size:15px; font-weight:700; color:#222;">${harborSize}</div>
                    </div>
                    <div>
                        <div style="font-size:11px; color:#999; text-transform:uppercase; letter-spacing:0.4px; margin-bottom:4px;">Harbor Type</div>
                        <div style="font-size:15px; font-weight:700; color:#222;">${harborType}</div>
                    </div>
                </div>
                ${disasterHTML}
            </div>
        </div>`;
}

function addPortMarker(port, color, label, nearLon) {
    const iconUrl = `https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-${color}.png`;
    const icon = L.icon({
        iconUrl,
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
        iconSize:    [25, 41],
        iconAnchor:  [12, 41],
        popupAnchor: [1, -34],
        shadowSize:  [41, 41]
    });

    let lon = port.lon;
    if (nearLon !== undefined) {
        while (lon - nearLon > 180) lon -= 360;
        while (nearLon - lon > 180) lon += 360;
    }

    const marker = L.marker([port.lat, lon], { icon }).addTo(map);
    marker.bindPopup('', { maxWidth: 280, closeOnClick: true });
    marker.on("click", function () {
        const disasters = window._getPortDisasters ? window._getPortDisasters(port) : [];
        marker.getPopup().setContent(buildPortPopup(port, label, disasters));
        marker.openPopup();
    });

    window.portMarkers.push(marker);
}