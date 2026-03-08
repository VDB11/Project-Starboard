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
    document.getElementById('route-info').style.display = 'none';
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
                `<div style="font-family: 'Roboto Slab', serif; font-size: 13px; padding: 4px 2px;">
                    <strong style="font-size:14px; color:#111;">${segment.from.port_name}</strong>
                    <span style="color: #4facfe; margin: 0 6px; font-size:16px;">⟶</span>
                    <strong style="font-size:14px; color:#111;">${segment.to.port_name}</strong>
                    <div style="color: #333; font-size: 12px; font-weight:700; margin-top: 4px;">${segment.length.toLocaleString()} nautical miles</div>
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

        document.getElementById('route-length').textContent =
            `Total Distance: ${data.total_length.toLocaleString()} nautical miles`;

        const segmentInfo = document.getElementById('segment-info');
        segmentInfo.innerHTML = data.segments.map((seg, i) =>
            `<div class="segment-item" style="border-left-color: ${SEGMENT_COLORS[i % SEGMENT_COLORS.length]}">
                <span><strong>${seg.from.port_name} → ${seg.to.port_name}</strong></span>
                <span>${seg.length.toLocaleString()} naut mi</span>
            </div>`
        ).join('');

        document.getElementById('route-info').style.display = 'block';

    } catch (err) {
        document.getElementById('loading').style.display = 'none';
        alert('Failed to calculate route: ' + err.message);
    }
});

function addPortMarker(port, color, label, nearLon) {
    const iconUrl = `https://raw.githubusercontent.com/pointhi/leaflet-color-markers/master/img/marker-icon-${color}.png`;
    const icon = L.icon({
        iconUrl,
        shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/0.7.7/images/marker-shadow.png',
        iconSize: [25, 41],
        iconAnchor: [12, 41],
        popupAnchor: [1, -34],
        shadowSize: [41, 41]
    });

    let lon = port.lon;
    if (nearLon !== undefined) {
        while (lon - nearLon > 180) lon -= 360;
        while (nearLon - lon > 180) lon += 360;
    }

    const marker = L.marker([port.lat, lon], { icon })
        .addTo(map)
        .bindPopup(`<strong>${label}</strong><br>${port.port_name}<br><small>${port.port_code || ''}</small>`);

    window.portMarkers.push(marker);
}